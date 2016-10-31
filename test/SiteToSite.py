#!/usr/bin/env
"""
Requirements:
-- fts3 python bindings installed
-- valid user proxy

Usage:

SiteToSite.py -i <inputfile.json> -d <destination> -s <source> -p <proxyPath>

example:

python SiteToSite.py -s "T3_IT_Perugia" -d "T2_IT_Pisa" -i "input.json" -p "/data/srv/asyncstageout/state/asyncstageout/creds/OpsProxy"

with input.json conten:
{"lfns":["/store/user/dciangot/WW_DoubleScattering_8TeV-pythia8/CRAB3_test_104/161013_132607/0000/output_72.root"]}

"""

import json
import sys
import urllib

import pycurl
from io import BytesIO
import fts3.rest.client.easy as fts3
from datetime import timedelta
import getopt


def apply_tfc_to_lfn(site, lfn, c):
    """
    Take a CMS_NAME:lfn string and make a pfn.
    :param site:
    :param lfn:
    :param c:
    :return:
    """

    # curl https://cmsweb.cern.ch/phedex/datasvc/json/prod/tfc?node=site
    # TODO: cache the tfc rules
    input_dict = {'node':site, 'lfn': lfn, 'protocol':"srmv2", 'custodial': 'n'}
    c.setopt(c.URL, 'https://cmsweb.cern.ch/phedex/datasvc/json/prod/lfn2pfn?'+urllib.urlencode(input_dict))
    e = BytesIO()
    c.setopt(pycurl.WRITEFUNCTION, e.write)
    c.perform()
    print(e.getvalue().decode('UTF-8'))
    results = json.loads(e.getvalue().decode('UTF-8'))

    e.close()
    return results["phedex"]["mapping"][0]["pfn"]


def chunks(l, n):
        """
        Yield successive n-sized chunks from l.
        :param l:
        :param n:
        :return:
        """
        for i in range(0, len(l), n):
            yield l[i:i + n]


def submit(proxy, toTrans, source, destination):

    # prepare rest job with 200 files per job
    transfers = []
    for files in chunks(toTrans, 200):

        c = pycurl.Curl()
        # create destination and source pfns for job
        for lfn in files:
            print(lfn)
            transfers.append(fts3.new_transfer(apply_tfc_to_lfn(source, lfn, c),
                                               apply_tfc_to_lfn(destination, lfn, c))
                            )

        c.close()

        # Submit fts job
        context = fts3.Context('https://fts3.cern.ch:8446', proxy, proxy, verify=True)
        print(fts3.delegate(context, lifetime=timedelta(hours=48), force=False))

        job = fts3.new_job(transfers)

        print("Monitor link: https://fts3.cern.ch:8449/fts3/ftsmon/#/job/"+fts3.submit(context, job))


def main(argv):
    inputfile = ""
    destination = ""
    source = ""
    proxy = ""
    try:
        opts, args = getopt.getopt(argv, "hi:d:s:p:", ["ifile=", "destination=", "source=", "proxy="])
    except getopt.GetoptError:
        print('SiteToSite.py -i <inputfile.json> -d <destination> -s <source> -p <proxyPath>')
        sys.exit(2)
    for opt, arg in opts:
        if opt == '-h':
            print('SiteToSite.py -i <inputfile.json> -d <destination> -s <source> -p <proxyPath>')
            sys.exit()
        elif opt in ("-i", "--ifile"):
            inputfile = arg
        elif opt in ("-d", "--destination"):
            destination = arg
        elif opt in ("-s", "--source"):
            source = arg
        elif opt in ("-p", "--proxy"):
            proxy = arg
    print('Input file is ', inputfile)
    print('Source: %s, destination: %s' %(source, destination))
    print('proxy: ', proxy)

    with open(inputfile) as data_file:
        data = json.load(data_file)
    # data = {"lfns": ["lfn1", "lfn2"...]}
    lfns = data["lfns"]

    submit(proxy, lfns, source, destination)

if __name__ == "__main__":
   main(sys.argv[1:])