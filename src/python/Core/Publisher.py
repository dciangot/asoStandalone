"""
- Get doc to publish
- Get user proxy
- publish
- update status
"""

import dbs.apis.dbsClient as dbsClient
from RestClient.ErrorHandling.RestClientExceptions import HTTPError
import os
import sys
import logging
import time
import json
from datetime import timedelta
from threading import Thread
from WMCore.Configuration import loadConfigurationFile
from MultiProcessingLog import MultiProcessingLog
from Core import setProcessLogger
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


class Publisher(object):
    """
    Get task to publish and update publish state
    """
    def __init__(self, config, quiet, debug, test=False):
        self.config_getter = config.Getter
        self.config = config.Publisher
        self.TEST = False

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

    def algorithm(self):
        """
        - acquire files for publication
        - get users to be published
        - queue ((user, group, role), [{task:'', lfns:[[source_lfn,dest_lfn]]}])
        - update publication status
        :return:
        """
        workers = list()
        for i in range(self.config.max_threads_num):
            worker = Thread(target=self.worker, args=(i, self.q))
            worker.setDaemon(True)
            worker.start()
            workers.append(worker)

        toPub = list()
        while not self.STOP:
            try:
                Update = update(self.logger, self.config_getter)
                self.logger.info("Acquiring publications")
                Update.acquirePub()
                toPub = Update.getPub()
                self.logger.info(str(len(toPub)) + "documents acquired")
            except Exception:
                self.logger.exception('Error during docs acquiring')
                continue

            users = [list(i) for i in set(tuple([x['username'], x['user_group'], x['user_role']])
                                          for x in toPub if x['transfer_state'] == 3)]
            self.logger.info('Active users: %s' % len(users))

            for user in users:
                tasks = [i for i in set([x['taskname'] for x in toPub
                                         if (x['username'], x['user_group'], x['user_role']) == user])]
                self.logger.info('%s active tasks for user %s' % (len(tasks), user))

            time.sleep(300)

        for w in workers:
            w.join()

        self.logger.info('Monitor stopped.')

    def worker(self, i, input):
        pass

    def quit_(self, dummyCode, dummyTraceback):
        self.logger.info("Received kill request. Setting STOP flag in the master and threads...")
        self.STOP = True

if __name__ == '__main__':
    """
    - get option and config monitor
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

    mw = Publisher(configuration, quiet=options.quiet, debug=options.debug)
    signal.signal(signal.SIGINT, mw.quit_)
    signal.signal(signal.SIGTERM, mw.quit_)
    mw.algorithm()