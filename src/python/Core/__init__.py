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


def checkLifeTimes(self, ProxyLife, VomsLife, proxy):
    """
    Evaluate the proxy validity comparing it with voms
    validity.
    """
    # TODO: make the minimum value between proxyLife and vomsLife configurable
    if abs(ProxyLife - VomsLife) > 900:
        hours = int(ProxyLife) / 3600
        minutes = (int(ProxyLife) - hours * 3600) / 60
        proxyLife = "%d:%02d" % (hours, minutes)
        hours = int(VomsLife) / 3600
        minutes = (int(VomsLife) - hours * 3600) / 60
        vomsLife = "%d:%02d" % (hours, minutes)
        msg = "Proxy lifetime %s is different from \
                voms extension lifetime %s for proxy %s" \
              % (proxyLife, vomsLife, proxy)
        self.logger.debug(msg)
        result = 0
    else:
        result = ProxyLife

    return result


# TODO: def getProxy(defaultDelegation, log): voms needed + sources
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


    proxyPath = os.path.join('/tmp', hashlib.sha1(defaultDelegation['userDN'] +
                                                       "cms" + defaultDelegation['group'] +
                                                       defaultDelegation['role']).hexdigest())

    timeLeftCmd = 'voms-proxy-info -file ' + proxyPath + ' -timeleft'
    timeLeftLocal, _, etcode = execute_command(timeLeftCmd)
    # TODO: exception and sources needed, command timeout?

    try:
        timeLeft = int(timeLeftLocal.strip())
    except ValueError:
        timeLeft = sum(int(x) * 60 ** i for i, x in enumerate(reversed(timeLeftLocal.strip().split(":"))))

    if timeLeft > 0:
        cmd = 'voms-proxy-info -file ' + proxyPath + ' -actimeleft'
        ACtimeLeftLocal, _, retcode = execute_command(cmd)
        # TODO: exception
        if ACtimeLeftLocal > 0:
            timeLeft = checkLifeTimes(timeLeft, ACtimeLeftLocal, proxyPath)
        else:
            timeLeft = 0

    return timeLeft


def vomsExtensionRenewal(self, proxy, voAttribute='cms'):
    """
    Renew voms extension of the proxy
    """
    ## get validity time for retrieved flat proxy
    cmd = 'grid-proxy-info -file ' + proxy + ' -timeleft'
    timeLeft, _, retcode = execute_command(cmd)

    if retcode != 0:
        self.log.error("Error while checking retrieved proxy timeleft for %s" % proxy)
        return

    vomsValid = '00:00'
    timeLeft = int(timeLeft.strip())

    if timeLeft > 0:
        vomsValid = "%d:%02d" % (timeLeft / 3600, (timeLeft - (timeLeft / 3600) * 3600) / 60)

    self.log.debug('Requested voms validity: %s' % vomsValid)

    msg, _, retcode = execute_command('voms-proxy-info -type -file %s' % proxy)
    if retcode > 0:
        self.log.error('Cannot get proxy type %s' % msg)
        return
    isRFC = msg.startswith('RFC')  # can be 'RFC3820 compliant impersonation proxy' or 'RFC compliant proxy'
    ## set environ and add voms extensions
    cmdList = []
    cmdList.append('env')
    cmdList.append('X509_USER_PROXY=%s' % proxy)
    cmdList.append('voms-proxy-init -noregen -voms %s -out %s -bits 1024 -valid %s %s'
                   % (voAttribute, proxy, vomsValid, '-rfc' if isRFC  else ''))
    cmd = ' '.join(cmdList)
    msg, _, retcode = execute_command(cmd)

    if retcode > 0:
        self.logger.error('Unable to renew proxy voms extension: %s' % msg)

    return


def getProxyDetails(self):
    """
    Return the vo details that should be in the user proxy.
    """
    proxyDetails = "/%s" % self.vo
    if self.group:
        proxyDetails += "/%s" % self.group
    if self.role and self.role != 'NULL':
        proxyDetails += "/Role=%s" % self.role

    return proxyDetails


def prepareAttForVomsRenewal(self, attribute='/cms'):
    """
    Prepare attribute for the voms renewal.
    """
    # prepare the attributes for voms extension
    voAttribute = self.vo + ':' + attribute

    # Clean attribute to extend voms
    voAttribute = voAttribute.replace('/Role=NULL', '')
    voAttribute = voAttribute.replace('/Capability=NULL', '')

    return voAttribute


def renewProxy(defaultDelegation, log):

    attribute = getProxyDetails()
    voAttribute = prepareAttForVomsRenewal(attribute)

    voAttribute = 'cms' + ':/' + attribute

    # Clean attribute to extend voms
    voAttribute = voAttribute.replace('/Role=NULL', '')
    voAttribute = voAttribute.replace('/Capability=NULL', '')
    cmdList = []
    cmdList.append('unset X509_USER_CERT X509_USER_KEY')
    cmdList.append('&& env')
    cmdList.append('X509_USER_CERT=%s' % defaultDelegation['serverCert'])
    cmdList.append('X509_USER_KEY=%s' % defaultDelegation['serverKey'])

    ## get a new delegated proxy
    proxyFilename = os.path.join( defaultDelegation['credServerPath'], hashlib.sha1(defaultDelegation['userDN'] +
                                                                            "cms" + defaultDelegation['group'] +
                                                                            defaultDelegation['role']).hexdigest())
    tmpProxyFilename = proxyFilename + '.' + str(os.getpid())

    cmdList.append('myproxy-logon -d -n -s %s -o %s -l \"%s\" -t 168:00'
                   % (defaultDelegation['myproxyServer'], tmpProxyFilename,
                      hashlib.sha1(defaultDelegation['userDN'] +
                                   "_" +
                                   defaultDelegation['myproxyAccount']).hexdigest()))

    logonCmd = ' '.join(cmdList)
    msg, _, retcode = execute_command(logonCmd)

    if retcode > 0:
        log.error("Unable to retrieve delegated proxy for user DN %s! Exit code:%s output:%s"
                  % (defaultDelegation['userDN'], retcode, msg))
        return proxyFilename

    vomsExtensionRenewal(tmpProxyFilename, voAttribute)
    return os.rename(tmpProxyFilename, proxyFilename)