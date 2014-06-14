#!/usr/bin/python
import logging
import optparse
import pprint
import sys

import db.interface
import extraction.recent_constraints
import extraction.turnover

# Requires eviction and address tables to have been built.
def report_turnover(writer, target_file = None):
    extraction.turnover.fetch(
        writer, target_file)

TARGETS = {
    "TURNOVER" : report_turnover
}

def parse_options():
    parser = optparse.OptionParser()
    target_list = ",".join(["\"%s\"" % k for k in  TARGETS.keys()])
    target_help = "Process ingested data for TARGET. TARGETs are:\n%s" % (
        target_list)
    parser.add_option("-t", "--target", dest="target",
                      help=target_help,
                      metavar="TARGET")
    parser.add_option("-f", "--file", dest="file",
                      help="(optional) Specify which file to write reports to.",
                      metavar="FILE")
    parser.add_option("-d", "--database", dest="database", default="DEV",
                      help="Database for digestion, as defined in secrets.py.",
                      metavar="DATABASE")
    
    opts, args = parser.parse_args()
    if opts.target not in TARGETS.keys():
        parser.print_help()
        sys.exit(2)
    return opts

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    opts = parse_options()
    db_interface = db.interface.DBInterface()
    db_interface.initialize(opts.database)
    TARGETS[opts.target](db_interface, target_file = opts.file)
