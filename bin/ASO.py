#!/usr/bin/env python

import multiprocessing
import sys
import os
from WMCore.Configuration import loadConfigurationFile
from Core.Getter import Getter
from Core.Monitor import Monitor
from Core.Publisher import Publisher
import signal

def quit():


if __name__ == '__main__':
    from optparse import OptionParser

    usage = "Usage: %prog --config=<config_file> [options] [args]"
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
    parser.add_option("--component",
                      dest="comp",
                      default=None,
                      help="list of component to be started")

    (options, args) = parser.parse_args()

    if options.config:
        print ('Please specify a configuration file. ')
        sys.exit(1)
    try:
        configuration = loadConfigurationFile(os.path.abspath(options.config))
    except Exception as ex:
        print ('Error during configuration parsing: ' + str(ex))
        sys.exit(1)

    if not options.comp:
        options.comp = ["Getter", "Monitor", "Publisher"]

    p = multiprocessing.Pool(len(options.comp))

    for component in options.comp:
        if component not in ["Getter", "Monitor", "Publisher"]:
            print >> sys.stderr, ('ERROR: %s is not a valid component name... skipping' % component)
            continue


        print ('Starting '+component)

        if component == "Getter":
            try:
                g = multiprocessing.Process(target=Getter,
                                            args=(configuration, options.quiet, options.debug))
                g.start()
            except Exception as ex:
                print >> sys.stderr, ('ERROR: starting %s . Exiting. %s' % (component,ex))
                sys.exit(1)

        elif component == "Monitor":
            try:
                g = multiprocessing.Process(target=Monitor,
                                            args=(configuration, options.quiet, options.debug))
                g.start()
            except Exception as ex:
                print >> sys.stderr, ('ERROR: starting %s . Exiting. %s' % (component,ex))
                sys.exit(1)
        elif component == "Publisher":
            try:
                p.multiprocessing.Process(target=Publisher,
                                            args=(configuration, options.quiet, options.debug))
                p.start()
            except Exception as ex:
                print >> sys.stderr, ('ERROR: starting %s . Exiting. %s' % (component,ex))
                sys.exit(1)

    signal.signal(signal.SIGINT, quit())
    signal.signal(signal.SIGTERM, quit())





