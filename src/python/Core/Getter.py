"""
- load configs
- Get users transfers and group by user and link
- launch submitter subprocess
- update status
"""
from RESTInteractions import HTTPRequests
from WMCore.Configuration import loadConfigurationFile
from ServerUtilities import encodeRequest, oracleOutputMapping
from MultiProcessingLog import MultiProcessingLog
from TaskWorker.Worker import setProcessLogger
from Core import Submitter
import logging
import sys
import os
import signal
import time


class Getter(object):
    """
    Get transfers to be submitted
    """
    def __init__(self, config, quiet, debug, test=False):
        """
        initialize
        :param config:
        :param quiet:
        :param debug:
        :param test:
        """
        self.oracleDB = HTTPRequests(self.config.oracleDB,
                                     self.config.opsProxy,
                                     self.config.opsProxy)

        def createLogdir(dirname):
            """ Create the directory dirname ignoring erors in case it exists. Exit if
                the directory cannot be created.
            """
            try:
                os.mkdir(dirname)
            except OSError as ose:
                if ose.errno != 17:  # ignore the "Directory already exists error"
                    print(str(ose))
                    print("The task worker need to access the '%s' directory" % dirname)
                    sys.exit(1)

        self.TEST = False

        def setRootLogger(quiet, debug):
            """Sets the root logger with the desired verbosity level
               The root logger logs to logs/asolog.txt and every single
               logging instruction is propagated to it (not really nice
               to read)

            :arg bool quiet: it tells if a quiet logger is needed
            :arg bool debug: it tells if needs a verbose logger
            :return logger: a logger with the appropriate logger level."""

            createLogdir('logs')
            createLogdir('logs/processes')
            createLogdir('logs/tasks')

            if self.TEST:
                # if we are testing log to the console is easier
                logging.getLogger().addHandler(logging.StreamHandler())
            else:
                logHandler = MultiProcessingLog('logs/asolog.txt', when='midnight')
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

        self.documents = {}
        self.doc_acq = ''
        self.STOP = False
        self.logger = setRootLogger(quiet, debug)
        self.slaves = Submitter()
        self.config = config
        self.slaves.begin()

    def algorithm(self):
        """
        - Get Users
        - Get Source dest
        - create queue for each (user, link)
        - create subprocesses
        """

        active_lfns = []
        while not self.STOP:
            sites, users = self.oracleSiteUser(self.oracleDB)

            for _user in users:
                for source in sites:
                    for dest in sites:
                        lfns = [x['lfn'] for x in self.documents
                                if x['source'] == source and x['dest'] == dest and x['username'] == _user[0] and
                                x not in active_lfns]
                        active_lfns = active_lfns + lfns
                        self.slaves.injectWorks(lfns, _user, source, dest, active_lfns)

            time.sleep(60)

        # TODO: store tfc rules, and remove from list lfn completed
        # for now inputs are just: lfns, dest, source, proxyPath

    def oracleSiteUser(self, db):
        """
        1. Acquire transfers from DB
        2. Get acquired users and destination sites
        """
        fileDoc = dict()
        fileDoc['asoworker'] = self.config.asoworker
        fileDoc['subresource'] = 'acquireTransfers'

        self.logger.debug("Retrieving transfers from oracleDB")

        try:
            result = db.post(self.config.oracleFileTrans,
                             data=encodeRequest(fileDoc))
        except Exception as ex:
            self.logger.error("Failed to acquire transfers \
                              from oracleDB: %s" % ex)
            pass

        self.doc_acq = str(result)

        fileDoc = dict()
        fileDoc['asoworker'] = self.config.asoworker
        fileDoc['subresource'] = 'acquiredTransfers'
        fileDoc['grouping'] = 0

        self.logger.debug("Retrieving users from oracleDB")

        try:
            results = db.get(self.config.oracleFileTrans,
                             data=encodeRequest(fileDoc))
            self.documents = oracleOutputMapping(results)
        except Exception as ex:
            self.logger.error("Failed to get acquired transfers \
                              from oracleDB: %s" % ex)
            pass

        for doc in self.documents:
            if doc['user_role'] is None:
                doc['user_role'] = ""
            if doc['user_group'] is None:
                doc['user_group'] = ""

        try:
            unique_users = [list(i) for i in
                            set(tuple([x['username'], x['user_group'], x['user_role']]) for x in self.documents)]
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

    def quit_(self, dummyCode, dummyTraceback):
        self.logger.info("Received kill request. Setting STOP flag in the master process...")
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

    # TODO: adapt it for ASO
    if not options.config:
        raise

    configuration = loadConfigurationFile(os.path.abspath(options.config))
    # TODO:
    # status_, msg_ = validateConfig(configuration)
    # if not status_:
    #     raise

    mw = Getter(configuration, quiet=options.quiet, debug=options.debug)
    signal.signal(signal.SIGINT, mw.quit_)
    signal.signal(signal.SIGTERM, mw.quit_)
    mw.algorithm()
    mw.slaves.end()

