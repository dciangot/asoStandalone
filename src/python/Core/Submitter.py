"""
- submit FTS3 jobs
- feed monitor+reporter
- update status
"""
import os
import time
import urllib
import logging
import traceback
import multiprocessing
from Queue import Empty
from base64 import b64encode
from logging import FileHandler
from httplib import HTTPException
from logging.handlers import TimedRotatingFileHandler

from ServerUtilities import truncateError
from RESTInteractions import HTTPRequests
from TaskWorker.DataObjects.Result import Result
from TaskWorker.WorkerExceptions import WorkerHandlerException

import fts3.rest.client.easy as fts3
from datetime import timedelta

from Database import update

from Core import getProxy
from Core import getHashLfn

def addTaskLogHandler(logger, username):
    # set the logger to save the tasklog
    formatter = logging.Formatter("%(asctime)s:%(levelname)s:%(module)s:%(message)s")
    taskdirname = "logs/users/%s/" % username
    try:
        os.mkdir(taskdirname)
    except OSError as ose:
        if ose.errno != 17: #ignore the "Directory already exists error" but print other errors traces
            logger.exception("Cannot set task handler logfile for task %s. Ignoring and continuing normally." % taskname)
    taskhandler = FileHandler(taskdirname + username + '.log')
    taskhandler.setFormatter(formatter)
    taskhandler.setLevel(logging.DEBUG)
    logger.addHandler(taskhandler)

    return taskhandler


def removeTaskLogHandler(logger, taskhandler):
    taskhandler.flush()
    taskhandler.close()
    logger.removeHandler(taskhandler)


def processWorkerLoop(inputs, results, procnum, logger):
    procName = "Process-%s" % procnum


def processWorker(inputs, results, procnum):
    """Wait for an reference to appear in the input queue, call the referenced object
       and write the output in the output queue.

       :arg Queue inputs: the queue where the inputs are shared by the master
       :arg Queue results: the queue where this method writes the output
       :return: default returning zero, but not really needed."""
    logger = setProcessLogger(str(procnum))
    logger.info("Process %s is starting. PID %s", procnum, os.getpid())
    try:
        processWorkerLoop(inputs, results, procnum, logger)
    except: #pylint: disable=bare-except
        #if enything happen put the log inside process logfiles instead of nohup.log
        logger.exception("Unexpected error in process worker!")
    logger.debug("Slave %s exiting.", procnum)
    return 0


def setProcessLogger(name):
    """ Set the logger for a single process. The file used for it is logs/processes/proc.name.txt and it
        can be retrieved with logging.getLogger(name) in other parts of the code
    """
    logger = logging.getLogger(name)
    handler = TimedRotatingFileHandler('logs/processes/proc.c3id_%s.pid_%s.txt' % (name, os.getpid()), 'midnight', backupCount=30)
    formatter = logging.Formatter("%(asctime)s:%(levelname)s:%(module)s:%(message)s")
    handler.setFormatter(formatter)
    logger.addHandler(handler)

    return logger

class Submitter(object):
    """Worker class providing all the functionalities to manage all the slaves
       and distribute the work"""

    def __init__(self, config):
        """

        :param config:
        """
        self.logger = logging.getLogger("master")
        self.pool = []
        self.nworkers = config.TaskWorker.nslaves if getattr(config.TaskWorker, 'nslaves', None) is not None else multiprocessing.cpu_count()
        # limit the size of the queue to be al maximum twice then the number of worker
        self.leninqueue = self.nworkers*2
        self.inputs = multiprocessing.Queue(self.leninqueue)
        self.results = multiprocessing.Queue()
        self.working = {}

    def begin(self):
        """Starting up all the slaves"""
        if len(self.pool) == 0:
            # Starting things up
            for x in xrange(1, self.nworkers + 1):
                self.logger.debug("Starting process %i" % x)
                p = multiprocessing.Process(target=processWorker, args=(self.inputs, self.results, x))
                p.start()
                self.pool.append(p)
        self.logger.info("Started %d slaves" % len(self.pool))

    def end(self):
        """Stopping all the slaves"""
        self.logger.debug("Ready to close all %i started processes " % len(self.pool))
        for p in self.pool:
            try:
                # Put len(self.pool) messages in the subprocesses queue.
                # Each subprocess will work on one stop message and exit
                self.logger.debug("Putting stop message in the queue for %s " % str(p))
                self.inputs.put(('-1', 'STOP', 'control', 'STOPFAILED', []))
            except Exception as ex: #pylint: disable=broad-except
                msg =  "Hit some exception in deletion\n"
                msg += str(ex)
                self.logger.error(msg)
        self.logger.info('Slaves stop messages sent. Waiting for subprocesses.')
        for p in self.pool:
            try:
                p.join()
            except Exception as ex: #pylint: disable=broad-except
                msg =  "Hit some exception in join\n"
                msg += str(ex)
                self.logger.error(msg)
        self.logger.info('Subprocesses ended!')

        self.pool = []
        return

    def injectWorks(self, items):
        """Takes care of iterating on the input works to do and
           injecting them into the queue shared with the slaves

           :arg list of tuple items: list of tuple, where each element
                                     contains the type of work to be
                                     done, the task object and the args."""
        self.logger.debug("Ready to inject %d items" % len(items))
        workid = 0 if len(self.working.keys()) == 0 else max(self.working.keys()) + 1
        for work in items:
            worktype, task, failstatus, arguments = work
            self.inputs.put(work)
            self.working[workid] = {'workflow': task['tm_taskname'], 'injected': time.time()}
            self.logger.info('Injecting work %d: %s' % (workid, task['tm_taskname']))
            workid += 1
        self.logger.debug("Injection completed.")

    def checkFinished(self):
        """Verifies if there are any finished jobs in the output queue

           :return Result: the output of the work completed."""
        if len(self.working.keys()) == 0:
            return []
        allout = []
        self.logger.info("%d work on going, checking if some has finished" % len(self.working.keys()))
        for _ in xrange(len(self.working.keys())):
            out = None
            try:
                out = self.results.get_nowait()
            except Empty:
                pass
            if out is not None:
                self.logger.debug('Retrieved work %s' % str(out))
                if isinstance(out['out'], list):
                    allout.extend(out['out'])
                else:
                    allout.append(out['out'])
                del self.working[out['workid']]
        return allout

    def freeSlaves(self):
        """Count how many unemployed slaves are there

        :return int: number of free slaves."""
        if self.queuedTasks() >= len(self.pool):
            return 0
        return len(self.pool) - self.queuedTasks()

    def queuedTasks(self):
        """Count how many busy slaves are out there

        :return int: number of working slaves."""
        return len(self.working)

    def queueableTasks(self):
        """Depending on the queue size limit
           return the number of free slots in
           the working queue.

           :return int: number of acquirable tasks."""
        if self.queuedTasks() >= self.leninqueue:
            return 0
        return self.leninqueue - self.queuedTasks()

    def pendingTasks(self):
        """Return the number of tasks pending
           to be processed and already in the
           queue.

           :return int: number of tasks waiting
                        in the queue."""
        if self.queuedTasks() <= len(self.pool):
            return 0
        return self.queuedTasks() - len(self.pool)


