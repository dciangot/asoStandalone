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
from datetime import timedelta
from threading import Thread, Lock
from WMCore.Configuration import loadConfigurationFile
from Queue import Queue

# class Master
# init etc
# def Queue per user

# while TRUE:
toMonitor = list()
for folder in os.listdir('Monitor'):
    user = folder
    jobs = os.listdir('Monitor/' + user)
    if not len(jobs) == 0:
        toMonitor.append(user)


#def worker per user
for job in os.listdir('Monitor/' + user):
    job = job.split('.')[0]
context = fts3.Context('https://fts3.cern.ch:8446', user_proxy, user_proxy, verify=True)
logger.debug(fts3.delegate(context, lifetime=timedelta(hours=48), force=False))