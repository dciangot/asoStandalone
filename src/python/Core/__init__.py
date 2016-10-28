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
    # TODO: make url configurable and use log
    c.setopt(c.URL, 'https://cmsweb.cern.ch/sitedb/data/prod/people?match=%s' % username)
    c.setopt(pycurl.SSL_VERIFYPEER, False)
    c.setopt(pycurl.SSLKEY, ckey)
    c.setopt(pycurl.SSLCERT, cert)
    c.setopt(pycurl.VERBOSE, 1)
    e = BytesIO()
    c.setopt(pycurl.WRITEFUNCTION, e.write)
    c.perform()
    # TODO: exception use log
    results = json.loads(e.getvalue().decode('UTF-8'))

    e.close()
    c.close()

    return results['dn']

# TODO: def getProxy(defaultDelegation, log): voms needed
def getProxy(defaultDelegation, log):
    """
    get user Proxy
    :param defaultDelegation:
    :param log:
    :return:
    """
    # TODO: test http://ndg-security.ceda.ac.uk/wiki/MyProxyClient
    # https://github.com/dmwm/WMCore/blob/master/src/python/WMCore/Credential/Proxy.py#69
    # get or renew proxy


    proxyPath = os.path.join(self.credServerPath, sha1(self.userDN + self.vo + self.group + self.role).hexdigest())

    timeLeftCmd = 'voms-proxy-info -file ' + proxyPath + ' -timeleft'
    timeLeftLocal, _, self.retcode = execute_command(self.setEnv(timeLeftCmd), self.logger, self.commandTimeout)
    # TODO: exception

    try:
        timeLeft = int(timeLeftLocal.strip())
    except ValueError:
        timeLeft = sum(int(x) * 60 ** i for i, x in enumerate(reversed(timeLeftLocal.strip().split(":"))))

    if timeLeft > 0:
        cmd = 'voms-proxy-info -file ' + proxyPath + ' -actimeleft'
        ACtimeLeftLocal, _, retcode = execute_command(self.setEnv(cmd), self.logger, self.commandTimeout)
        # TODO: exception
        if ACTimeLeftLocal > 0:
            timeLeft = self.checkLifeTimes(timeLeft, ACTimeLeftLocal, proxy)
        else:
            timeLeft = 0

    if timeLeft is not None and timeleft > 3600:
        return (True, proxyPath)

    # otherwise renew it
    # proxyPath = proxy.logonRenewMyProxy()
    # timeleft = proxy.getTimeLeft(proxyPath)
    # if timeleft is not None and timeleft > 0:...




# TODO: LOGGING
