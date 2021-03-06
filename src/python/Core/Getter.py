"""
- load configs
- Get users transfers and group by user and link
- launch submitter subprocess
- update status
"""
from WMCore.Configuration import loadConfigurationFile
from MultiProcessingLog import MultiProcessingLog
from WMCore.Services.PhEDEx.PhEDEx import PhEDEx
from Core import getDNFromUserName
from Core import getProxy
from threading import Thread, Lock
from datetime import timedelta
import fts3.rest.client.easy as fts3
from WMCore.Storage.TrivialFileCatalog import readTFC
import logging
import sys
import os
import signal
import time
from Queue import Queue
import re
from Core.Database.update import update
from Core import setProcessLogger, chunks, Submission
from Core import getHashLfn
import json

# TODO: comment threading and docstring


def createLogdir(dirname):
    """
    Create the directory dirname ignoring errors in case it exists. Exit if
    the directory cannot be created.
    """
    try:
        os.mkdir(dirname)
    except OSError as ose:
        if ose.errno != 17:  # ignore the "Directory already exists error"
            print(str(ose))
            print("The worker need to access the '%s' directory" % dirname)
            sys.exit(1)


class Getter(object):
    """
    Get transfers to be submitted
    """
    def __init__(self, config, quiet, debug, test=False):
        """
        initialize log, connections etc
        """
        self.config = config.Getter

        self.TEST = False
        createLogdir('Monitor')

        def setRootLogger(quiet, debug):
            """
            Taken from CRABServer TaskWorker
            Sets the root logger with the desired verbosity level
               The root logger logs to logs/asolog.txt and every single
               logging instruction is propagated to it (not really nice
               to read)

            :arg bool quiet: it tells if a quiet logger is needed
            :arg bool debug: it tells if needs a verbose logger
            :return logger: a logger with the appropriate logger level."""

            createLogdir('logs')
            createLogdir('logs/processes')

            if self.TEST:
                # if we are testing log to the console is easier
                logging.getLogger().addHandler(logging.StreamHandler())
            else:
                logHandler = MultiProcessingLog('logs/submitter.txt', when='midnight')
                logFormatter = \
                    logging.Formatter("%(asctime)s:%(levelname)s:%(module)s:%(message)s")
                logHandler.setFormatter(logFormatter)
                logging.getLogger().addHandler(logHandler)
            loglevel = logging.INFO
            if quiet:
                loglevel = logging.WARNING
            if debug:
                loglevel = logging.DEBUG
            logging.getLogger().setLevel(loglevel)
            logger = setProcessLogger("master")
            logger.debug("PID %s.", os.getpid())
            logger.debug("Logging level initialized to %s.", loglevel)
            return logger

        try:
            self.phedex = PhEDEx(responseType='xml',
                                 dict={'key': self.config.opsProxy,
                                       'cert': self.config.opsProxy})
        except Exception as e:
            self.logger.exception('PhEDEx exception: %s' % e)

        self.documents = dict()
        self.doc_acq = ''
        self.STOP = False
        self.logger = setRootLogger(quiet, debug)
        self.q = Queue()
        self.active_lfns = list()
        self.Update = update(self.logger, self.config)
        self.site_tfc_map = {}
        for site in [x['name'] for x in json.loads(self.phedex.getNodeMap())['phedex']['node']]:
            if site and str(site) != 'None' and str(site) != 'unknown':
                self.site_tfc_map[site] = self.get_tfc_rules(site)
                self.logger.debug('tfc site: %s %s' % (site, self.get_tfc_rules(site)))


    def algorithm(self):
        """
        - Get Users
        - Get Source dest
        - create queue for each (user, link)
        - feed threads
        """

        workers = list()
        for i in range(self.config.max_threads_num):
            worker = Thread(target=self.worker, args=(i, self.q))
            worker.setDaemon(True)
            worker.start()
            workers.append(worker)

        site_tfc_map = dict()
        while not self.STOP:
            sites, users = self.oracleSiteUser(self.Update)

            self.Update.retry()

            for _user in users:
                for source in sites:
                    for dest in sites:
                        lfns = [[x['source_lfn'], x['destination_lfn']] for x in self.documents
                                if x['source'] == source and x['destination'] == dest and x['username'] == _user[0] and
                                x not in self.active_lfns]
                        self.active_lfns = self.active_lfns + lfns
                        # IMPORTANT: remove only on final states

                        for files in chunks(lfns, self.config.files_per_job):
                            self.q.put((files, _user, source, dest, self.site_tfc_map))

            self.logger.debug('Queue lenght: %s' % self.q.qsize())
            time.sleep(4)

        for w in workers:
            w.join()

        self.logger.info('Submitter stopped.')

    def oracleSiteUser(self, Update):
        """
        1. Acquire transfers from DB
        2. Get acquired users and destination sites
        """

        # TODO: flexible with other DBs and get users list

        users = Update.acquire()

        if users != 1:
            self.documents = Update.getAcquired(users)

        for doc in self.documents:
            if doc['user_role'] is None:
                doc['user_role'] = ""
            if doc['user_group'] is None:
                doc['user_group'] = ""

        unique_users = list()
        try:
            unique_users = [list(i) for i in
                            set(tuple([x['username'],
                                       x['user_group'],
                                       x['user_role']]) for x in self.documents)]
        except Exception as ex:
            self.logger.error("Failed to map active users: %s" % ex)

        if len(unique_users) <= self.config.pool_size:
            active_users = unique_users
        else:
            active_users = unique_users[:self.config.pool_size]

        self.logger.info('%s active users' % len(active_users))
        self.logger.debug('Active users are: %s' % active_users)

        active_sites_dest = [x['destination'] for x in self.documents]
        active_sites = active_sites_dest + [x['source'] for x in self.documents]

        self.logger.debug('Active sites are: %s' % list(set(active_sites)))
        return list(set(active_sites)), active_users

    def get_tfc_rules(self, site):
        """
        Get the TFC regexp for a given site.
        """
        tfc_file = None
        try:
            self.phedex.getNodeTFC(site)
        except Exception as e:
            self.logger.exception('PhEDEx exception: %s' % e)
        try:
            tfc_file = self.phedex.cacheFileName('tfc',
                                                 inputdata={'node': site})
        except Exception as e:
            self.logger.exception('PhEDEx cache exception: %s' % e)
        return readTFC(tfc_file)

    def critical_failure(self, lfns, lock, inputs):
        """
        if an exception occurs before the end, remove lfns from active
        to let it be reprocessed later.

        :param lfns:
        :param lock:
        :param inputs:
        :return:
        """
        lock.acquire()
        for lfn in lfns:
            self.active_lfns.remove(lfn)
        lock.release()
        inputs.task_done()

    def worker(self, i, inputs):
        """
        - Retrieve userDN
        - Retrieve user proxy
        - Delegate proxy to fts is needed
        - submit fts job
        - update doc states

        :param i: thread number
        :param inputs: tuple (lfns, _user, source, dest, tfc_map)
        :return:
        """
        # TODO: differentiate log messages per USER!
        logger = self.logger
        logger.info("Process %s is starting. PID %s", i, os.getpid())
        lock = Lock()
        Update = update(logger, self.config)

        while not self.STOP:
            if inputs.empty():
                time.sleep(10)
                continue
            try:
                lfns, _user, source, dest, tfc_map = inputs.get()
                [user, group, role] = _user
            except (EOFError, IOError):
                crashMessage = "Hit EOF/IO in getting new work\n"
                crashMessage += "Assuming this is a graceful break attempt.\n"
                logger.error(crashMessage)
                continue

            start = time.time()

            if not self.config.TEST:
                try:
                    userDN = getDNFromUserName(user, logger, ckey=self.config.opsProxy, cert=self.config.opsProxy)
                except Exception as ex:
                    logger.exception('Cannot retrieve user DN')
                    self.critical_failure(lfns, lock, inputs)
                    continue

                defaultDelegation = {'logger': logger,
                                     'credServerPath': self.config.credentialDir,
                                     'myProxySvr': 'myproxy.cern.ch',
                                     'min_time_left': getattr(self.config, 'minTimeLeft', 36000),
                                     'serverDN': self.config.serverDN,
                                     'uisource': '',
                                     'cleanEnvironment': getattr(self.config, 'cleanEnvironment', False)}

                cache_area = self.config.cache_area

                try:
                    defaultDelegation['myproxyAccount'] = re.compile('https?://([^/]*)/.*').findall(cache_area)[0]
                except IndexError:
                    logger.error('MyproxyAccount parameter cannot be retrieved from %s . ' % self.config.cache_area)
                    self.critical_failure(lfns, lock, inputs)
                    continue

                if getattr(self.config, 'serviceCert', None):
                    defaultDelegation['server_cert'] = self.config.serviceCert
                if getattr(self.config, 'serviceKey', None):
                    defaultDelegation['server_key'] = self.config.serviceKey

                try:
                    defaultDelegation['userDN'] = userDN
                    defaultDelegation['group'] = group
                    defaultDelegation['role'] = role
                    logger.debug('delegation: %s' % defaultDelegation)
                    valid_proxy, user_proxy = getProxy(defaultDelegation, logger)
                    if not valid_proxy:
                        logger.error('Failed to retrieve user proxy... putting docs on retry')
                        logger.error('docs on retry: %s' % Update.failed(lfns, submission_error=True))
                        continue
                except Exception:
                    logger.exception('Error retrieving proxy')
                    self.critical_failure(lfns, lock, inputs)
                    continue
            else:
                user_proxy = self.config.opsProxy
                self.logger.debug("Using opsProxy for testmode")

            context = dict()
            try:
                if self.config.TEST:
                    logger.debug("Running in test mode, submitting fake jobs")
                else:
                    context = fts3.Context(self.config.serverFTS, user_proxy, user_proxy, verify=True)
                    logger.debug(fts3.delegate(context, lifetime=timedelta(hours=48), force=False))
            except Exception:
                logger.exception("Error submitting to FTS")
                self.critical_failure(lfns, lock, inputs)
                continue

            failed_lfn = list()
            try:
                if self.config.TEST:
                    submitted_lfn = lfns
                    jobid = getHashLfn(lfns[0][0])
                    self.logger.debug('Fake job id: ' + jobid)
                else:
                    failed_lfn, submitted_lfn, jobid = Submission(lfns, source, dest, i,
                                                                  self.logger, fts3, context, tfc_map)
                    if jobid == -1:
                        self.critical_failure(lfns, lock, inputs)
                        continue
                    logger.info('Submitted %s files' % len(submitted_lfn))
            except Exception:
                logger.exception("Unexpected error during FTS job submission!")
                self.critical_failure(lfns, lock, inputs)
                continue

            # TODO: add file FTS id and job id columns for kill command
            try:
                Update.submitted(lfns)
            except Exception:
                logger.exception("Error updating document status")
                self.critical_failure(lfns, lock, inputs)
                continue

            try:
                Update.failed(failed_lfn)
            except Exception:
                logger.exception("Error updating document status, job submission will be retried later...")
                self.critical_failure(lfns, lock, inputs)
                continue

            try:
                createLogdir('Monitor/'+user)
                with open('Monitor/'+user+'/'+str(jobid)+'.txt', 'w') as outfile:
                    json.dump(lfns, outfile)
                logger.info('Monitor files created')
            except Exception:
                logger.exception("Error creating file for monitor")
                self.critical_failure(lfns, lock, inputs)
                continue

            end = time.time()
            self.logger.info('Input processed in %s', str(end-start))
            time.sleep(0.5)

        logger.debug("Worker %s exiting.", i)
        return 0

    def quit_(self):
        """
        set STOP to True
        :return:
        """
        self.logger.info("Received kill request. Setting STOP flag in the master and threads...")
        self.STOP = True


if __name__ == '__main__':
    """
    Taken from CRABServer TaskWorker
    - get option and config getter
    """

    from optparse import OptionParser

    usage = "usage: %prog [options] [args]"
    parser = OptionParser(usage=usage)

    parser.add_option("-d", "--debug",
                      action="store_true",
                      dest="debug",
                      default=False,
                      help="print extra messages to stdout" )
    parser.add_option("-q", "--quiet",
                      action="store_true",
                      dest="quiet",
                      default=False,
                      help="don't print any messages to stdout" )

    parser.add_option("--config",
                      dest="config",
                      default=None,
                      metavar="FILE",
                      help="configuration file path" )

    (options, args) = parser.parse_args()

    # TODO: adapt evaluation it for ASO
    if not options.config:
        raise

    configuration = loadConfigurationFile(os.path.abspath(options.config))

    mw = Getter(configuration, quiet=options.quiet, debug=options.debug)
    signal.signal(signal.SIGINT, mw.quit_)
    signal.signal(signal.SIGTERM, mw.quit_)
    mw.algorithm()
