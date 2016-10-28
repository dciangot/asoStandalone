import os
import time
import logging
import hashlib
import subprocess
import getopt
import pycurl
import json
import urllib
from io import BytesIO

__version__ = '1.0.3'


def getHashLfn(lfn):
    """
    Provide a hashed lfn from an lfn.
    """
    return hashlib.sha224(lfn).hexdigest()


def execute_command(command):
    """
    _execute_command_
    Function to manage commands.
    """
    proc = subprocess.Popen(
           ["/bin/bash"], shell=True, cwd=os.environ['PWD'],
           stdout=subprocess.PIPE,
           stderr=subprocess.PIPE,
           stdin=subprocess.PIPE,
    )
    proc.stdin.write(command)
    stdout, stderr = proc.communicate()
    rc = proc.returncode

    return stdout, stderr, rc

# TODO: def getDNFromUserName(username, log, ckey = None, cert = None)
def getDNFromUserName(username, log, ckey = None, cert = None):
    """
    get DN for user from siteDB
    :param username:
    :param log:
    :param ckey:
    :param cert:
    :return:
    """
    c = pycurl.Curl()
    # TODO: make url configurable
    c.setopt(c.URL, 'https://cmsweb.cern.ch/sitedb/data/prod/people?match=%s' % username)
    c.setopt(pycurl.SSL_VERIFYPEER, False)
    c.setopt(pycurl.SSLKEY, ckey)
    c.setopt(pycurl.SSLCERT, cert)
    c.setopt(pycurl.VERBOSE, 1)
    e = BytesIO()
    c.setopt(pycurl.WRITEFUNCTION, e.write)
    c.perform()
    # TODO: exception
    results = json.loads(e.getvalue().decode('UTF-8'))

    return results['dn']

# TODO: def getProxy(defaultDelegation, log)



# TODO: LOGGING
