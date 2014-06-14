#!/usr/bin/python
import logging
import optparse
import pprint
import sys

import db.interface
import digestion.locations.resolve_eviction_addresses
import digestion.locations.resolve_zoning_gps

# Requires eviction and address tables to have been built.
def digest_eviction_tables(writer, target_file = None):
    digestion.locations.resolve_eviction_addresses.augment_evictions(
        writer, target_file)

# Requires address table to have been built.
def digest_zoning_rules(writer, target_file = None):
    digestion.locations.resolve_zoning_gps.resolve_zoning_rules(
        writer, target_file)

TARGETS = {
    "EVICTION" : digest_eviction_tables,
    "ZONING" : digest_zoning_rules
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
                      help="(optional) Specify which file to write errors to.",
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
