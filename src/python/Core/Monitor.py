"""
- Get FTS jobs
- Get user proxy
- Monitor user transfers
- Update status
- Remove files from source
- Feed Publisher if needed
"""
import fts3.rest.client.easy as fts3
import os
import sys
import logging
import time
import json
from datetime import timedelta
from threading import Thread, Lock
from WMCore.Configuration import loadConfigurationFile
from MultiProcessingLog import MultiProcessingLog
from Core import setProcessLogger
from RESTInteractions import HTTPRequests
from Queue import Queue
from Core.Database.update import update
import signal


def createLogdir(dirname):
    """ Create the directory dirname ignoring errors in case it exists. Exit if
        the directory cannot be created.
    """
    try:
        os.mkdir(dirname)
    except OSError as ose:
        if ose.errno != 17:  # ignore the "Directory already exists error"
            print(str(ose))
            print("The worker need to access the '%s' directory" % dirname)
            sys.exit(1)


class Monitor(object):
    """
    Monitor user FTS job and update states
    """
    def __init__(self, config, quiet, debug, test=False):
        """

        :param config:
        :param quiet:
        :param debug:
        :param test:
        """
        self.config_getter = config.Getter
        self.config = config.Monitor
        self.TEST = False

        createLogdir('Done')

        def setRootLogger(quiet, debug):
            """Sets the root logger with the desired verbosity level
               The root logger logs to logs/asolog.txt and every single
               logging instruction is propagated to it (not really nice
               to read)

            :arg bool quiet: it tells if a quiet logger is needed
            :arg bool debug: it tells if needs a verbose logger
            :return logger: a logger with the appropriate logger level."""

            createLogdir('logs')

            if self.TEST:
                # if we are testing log to the console is easier
                logging.getLogger().addHandler(logging.StreamHandler())
            else:
                logHandler = MultiProcessingLog('logs/monitor.txt', when='midnight')
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

        self.STOP = False
        self.logger = setRootLogger(quiet, debug)
        self.active_users = list()
        self.q = Queue()
        self.context = fts3.Context(self.config_getter.serverFTS,
                                    self.config_getter.opsProxy,
                                    self.config_getter.opsProxy, verify=True)

    def algorithm(self):
        """

        :return:
        """
        workers = list()
        for i in range(self.config.max_threads_num):
            worker = Thread(target=self.worker, args=(i, self.q))
            worker.setDaemon(True)
            worker.start()
            workers.append(worker)

        count = 0
        while not self.STOP:
            if count == 0:
                self.context = fts3.Context(self.config_getter.serverFTS,
                                            self.config_getter.opsProxy,
                                            self.config_getter.opsProxy, verify=True)
                self.logger.debug(fts3.delegate(self.context, lifetime=timedelta(hours=48), force=False))

            for folder in os.listdir('Monitor'):
                user = folder
                jobs = os.listdir('Monitor/' + user)
                if not len(jobs) == 0 and user not in self.active_users:
                    self.active_users.append(user)
                    self.q.put(user)
                elif len(jobs) == 0 and user in self.active_users:
                    self.active_users.remove(user)

            if count < 6*60*12:  # delegate every 12h
                count += 1
            else:
                count = 0
            self.logger.info('%s active users' % len(self.active_users))
            self.logger.debug('Active users are: %s' % self.active_users)
            self.logger.debug('Queue lenght: %s' % self.q.qsize())
            time.sleep(300)

        for w in workers:
            w.join()

        self.logger.info('Monitor stopped.')

    def worker(self, i, input):
        """

        :param i:
        :param inputs:
        :return:
        """
        oracleDB = HTTPRequests(self.config_getter.oracleDB,
                                self.config_getter.opsProxy,
                                self.config_getter.opsProxy)

        context = fts3.Context(self.config_getter.serverFTS,
                               self.config_getter.opsProxy,
                               self.config_getter.opsProxy, verify=True)

        logger = self.logger  # setProcessLogger('Mon'+str(i))
        logger.info("Process %s is starting. PID %s", i, os.getpid())
        lock = Lock()
        Update = update(logger, oracleDB, self.config_getter)

        while not self.STOP:
            if input.empty():
                time.sleep(10)
                continue
            try:
                user = input.get()
            except (EOFError, IOError):
                crashMessage = "Hit EOF/IO in getting new work\n"
                crashMessage += "Assuming this is a graceful break attempt.\n"
                logger.error(crashMessage)
                break

            for File in os.listdir('Monitor/' + user):
                job = File.split('.')[0]
                try:
                    results = fts3.get_job_status(context, job, list_files=True)

                    self.logger.info('Getting status for job: ' + job + ' ' + results['job_state'])
                except Exception:
                    logger.exception('Failed get job status for %s' % job)
                    continue

                if results['job_state'] in ('FINISHED',
                                            'FAILED',
                                            'FINISHEDDIRTY',
                                            'CANCELED'):

                    failed_lfn = list()
                    failed_reasons = list()
                    done_lfn = list()
                    for Fl in results['files']:
                        lfn = Fl['file_metadata']['lfn']
                        if Fl['file_state'] == 'FINISHED':
                            done_lfn.append(lfn)
                        else:
                            failed_lfn.append(lfn)
                            if Fl['reason'] is not None:
                                self.logger.warning('Failure reason: ' + Fl['reason'])
                                failed_reasons.append(Fl['reason'])
                            else:
                                self.logger.exception('Failure reason not found')
                                failed_reasons.append('unable to get failure reason')

                    try:
                        logger.info('Marking job %s files done and %s files  failed for job %s'
                                    % (len(done_lfn), len(failed_lfn), job))
                        Update.transferred(done_lfn)
                        Update.failed(failed_lfn, failed_reasons)
                    except Exception:
                        logger.exception('Failed to update states')
                        continue

                    try:
                        logger.info('Removing' + 'Monitor/' + user + '/' + File)
                        os.rename('Monitor/' + user + '/' + File, 'Done/' + File)
                    except:
                        logger.exception('failed to remove monitor file')
                        continue

            input.task_done()
            self.active_users.remove(user)
            time.sleep(10)
        logger.debug("Worker %s exiting.", i)
                # TODO: cleaner

    def quit_(self, dummyCode, dummyTraceback):
        self.logger.info("Received kill request. Setting STOP flag in the master and threads...")
        self.STOP = True


if __name__ == '__main__':
    """
    - get option and config masterworker
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

    mw = Monitor(configuration, quiet=options.quiet, debug=options.debug)
    signal.signal(signal.SIGINT, mw.quit_)
    signal.signal(signal.SIGTERM, mw.quit_)
    mw.algorithm()


