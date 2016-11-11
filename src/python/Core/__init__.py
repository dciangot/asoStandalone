import os
import hashlib
import subprocess
import time
import logging
from WMCore.Services.SiteDB.SiteDB import SiteDBJSON
from WMCore.Credential.Proxy import Proxy
from logging.handlers import TimedRotatingFileHandler


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
    Parse site string to know the fts server to use
    """
    dn = ''
    site_db = SiteDBJSON(config={'key': ckey, 'cert': cert})
    try:
        dn = site_db.userNameDn(username)
    except IndexError:
        log.error("user does not exist")
        return dn
    except RuntimeError:
        log.error("SiteDB URL cannot be accessed")
        return dn
    return dn


def getProxy(defaultDelegation, log):
    """
    _getProxy_
    """
    log.debug("Retrieving proxy for %s" % defaultDelegation['userDN'])
    proxy = Proxy(defaultDelegation)
    proxyPath = proxy.getProxyFilename( True )
    timeleft = proxy.getTimeLeft(proxyPath)
    if timeleft is not None and timeleft > 3600:
        return True, proxyPath
    proxyPath = proxy.logonRenewMyProxy()
    timeleft = proxy.getTimeLeft(proxyPath)
    if timeleft is not None and timeleft > 0:
        return True, proxyPath
    return False, None

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


def chunks(l, n):
    """
    Yield successive n-sized chunks from l.
    :param l:
    :param n:
    :return:
    """
    for i in range(0, len(l), n):
        yield l[i:i + n]


def apply_tfc_to_lfn(tfc_map, logger, file=tuple()):
    """
    Take a CMS_NAME:lfn string and make a pfn.
    Update pfn_to_lfn_mapping dictionary.
    """
    try:
        site, lfn = file
    except:
        logger.error('it does not seem to be an lfn %s' % file.split(':'))
        return None
    if site in tfc_map:
        pfn = tfc_map[site].matchLFN('srmv2', lfn)
        # TODO: improve fix for wrong tfc on sites
        try:
            if pfn.find("\\") != -1: pfn = pfn.replace("\\", "")
            if len(pfn.split(':')) == 1:
                logger.error('Broken tfc for file %s at site %s' % (lfn, site))
                return None
        except IndexError:
            logger.error('Broken tfc for file %s at site %s' % (lfn, site))
            return None
        except AttributeError:
            logger.error('Broken tfc for file %s at site %s' % (lfn, site))
            return None
        return pfn
    else:
        logger.error('Wrong site %s!' % site)
        return None


def Submission(lfns, source, dest, procnum, logger, fts3, tfc_map):
    procName = "Process-%s" % procnum

    t0 = time.time()
    logger.debug("Starting process %s ", procName)

    transfers = list()

    failed_lfn = list()
    submitted_lfn = list()
    # TODO: Exception and group lfns!
    for lfn in lfns:
        print(lfn)
        try:
            transfers.append(fts3.new_transfer(apply_tfc_to_lfn(tfc_map, logger, (source, lfn)),
                                               apply_tfc_to_lfn(tfc_map, logger, (dest, lfn)))
                             )
        except Exception:
            logger.exception("Error creating new transfer")
            failed_lfn.append(lfn)
            continue

        submitted_lfn.append(lfn)

    try:
        job = fts3.new_job(transfers)
        # TODO: use different fts3 exceptions
    except Exception:
        logger.exception("Error submitting jobs to fts")
        failed_lfn = lfns
        del submitted_lfn

    # TODO: register jobid, lfns, user per monitor
    t1 = time.time()
    logger.debug("%s: ...work completed in %d seconds", job, t1 - t0)
    return failed_lfn, submitted_lfn, job