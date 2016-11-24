#!/usr/bin/env python

import subprocess
import os
from WMCore.Configuration import loadConfigurationFile


class ASO(object):
    def __init__(self):
        pass

    def algo(self):
        pass


if __name__ == '__main__':
    from optparse import OptionParser

    usage = "usage: %prog [options] [args]"
    parser = OptionParser(usage=usage)

    parser.add_option("-d", "--debug",
                      action="store_true",
                      dest="debug",
                      default=False,
                      help="print extra messages to stdout")
    parser.add_option("-q", "--quiet",
                      action="store_true",
                      dest="quiet",
                      default=False,
                      help="don't print any messages to stdout")

    parser.add_option("--config",
                      dest="config",
                      default=None,
                      metavar="FILE",
                      help="configuration file path")

    (options, args) = parser.parse_args()

    configuration = loadConfigurationFile(os.path.abspath(options.config))

    main = ASO()
