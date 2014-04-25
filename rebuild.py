#!/usr/bin/python
import logging
import optparse
import pprint
import sys

import db.interface
import ingestion.address.ingest
import ingestion.record.ingest

XSLX_FILE = 'data/address/EAS_address_with_blklot.xlsx'
JSON_RECORD_FILES = 'data/record/deeds/*.json'

def rebuild_address_tables(writer):
    return ingestion.address.ingest.ingest(XSLX_FILE, writer)

def rebuild_record_tables(writer):
    return ingestion.record.ingest.ingest(JSON_RECORD_FILES, writer)

TARGETS = {
    "ADDRESS" : rebuild_address_tables,
    "RECORD" : rebuild_record_tables
}

def parse_options():
    parser = optparse.OptionParser()
    target_list = ",".join(["\"%s\"" % k for k in  TARGETS.keys()])
    target_help = "Ingest and rebuild tables for TARGET. TARGETs are:\n%s" % (
        target_list)
    parser.add_option("-t", "--target", dest="target",
                      help=target_help,
                      metavar="TARGET")
    opts, args = parser.parse_args()
    if opts.target not in TARGETS.keys():
        parser.print_help()
        sys.exit(2)
    return opts.target

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    target_name = parse_options()
    db_interface = db.interface.DBInterface()
    db_interface.initialize()
    TARGETS[target_name](db_interface)




