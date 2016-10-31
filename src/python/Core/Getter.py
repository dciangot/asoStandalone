"""
- load configs
- Get users transfers and group by user and link
- launch submitter subprocess
- update status
"""
from RESTInteractions import HTTPRequests
from ServerUtilities import encodeRequest, oracleOutputMapping
from MultiProcessingLog import MultiProcessingLog
from TaskWorker.Worker import setProcessLogger
from Core import Submitter
import logging
import sys
import os


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

        sites, users = self.oracleSiteUser(self.oracleDB)
        # TODO: store tfc rules
        # for now inputs are just: lfns, dest, source, proxyPath

        # while(not self.STOP):
        #
        # self.slaves.injectWorks(toInject)


    def oracleSiteUser(self, db):
        """
        1. Acquire transfers from DB
        2. Get acquired users and destination sites
        """
        fileDoc = {}
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

        fileDoc = {}
        fileDoc['asoworker'] = self.config.asoworker
        fileDoc['subresource'] = 'acquiredTransfers'
        fileDoc['grouping'] = 0

        self.logger.debug("Retrieving users from oracleDB")

        documents = {}
        try:
            results = db.get(self.config.oracleFileTrans,
                             data=encodeRequest(fileDoc))
            documents = oracleOutputMapping(results)
        except Exception as ex:
            self.logger.error("Failed to get acquired transfers \
                              from oracleDB: %s" % ex)
            pass

        documents = oracleOutputMapping(results)

        for doc in documents:
            if doc['user_role'] is None:
                doc['user_role'] = ""
            if doc['user_group'] is None:
                doc['user_group'] = ""

        try:
            unique_users = [list(i) for i in
                            set(tuple([x['username'], x['user_group'], x['user_role']]) for x in documents)]
        except Exception as ex:
            self.logger.error("Failed to map active users: %s" % ex)

        if len(unique_users) <= self.config.pool_size:
            active_users = unique_users
        else:
            active_users = unique_users[:self.config.pool_size]

        self.logger.info('%s active users' % len(active_users))
        self.logger.debug('Active users are: %s' % active_users)

        active_sites_dest = [x['destination'] for x in documents]
        active_sites = active_sites_dest + [x['source'] for x in documents]
        try:
            self.kibana_file.write(self.doc_acq + "\n")
        except Exception as ex:
            self.logger.error(ex)

        self.logger.debug('Active sites are: %s' % list(set(active_sites)))
        return list(set(active_sites)), active_users

if __name__ == '__main__':
    """
    -
    """


"""
    from optparse import OptionParser

    usage  = "usage: %prog [options] [args]"
    parser = OptionParser(usage=usage)

    parser.add_option("-d","--debug",
                       action = "store_true",
                       dest = "debug",
                       default = False,
                       help = "print extra messages to stdout" )
    parser.add_option( "-q", "--quiet",
                       action = "store_true",
                       dest = "quiet",
                       default = False,
                       help = "don't print any messages to stdout" )

    parser.add_option( "--config",
                       dest = "config",
                       default = None,
                       metavar = "FILE",
                       help = "configuration file path" )

    (options, args) = parser.parse_args()


    if not options.config:
        raise ConfigException("Configuration not found")

    configuration = loadConfigurationFile( os.path.abspath(options.config) )
    status_, msg_ = validateConfig(configuration)
    if not status_:
        raise ConfigException(msg_)

    mw = MasterWorker(configuration, quiet=options.quiet, debug=options.debug)
    signal.signal(signal.SIGINT, mw.quit_)
    signal.signal(signal.SIGTERM, mw.quit_)
    mw.algorithm()
    mw.slaves.end()
"""
