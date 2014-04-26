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

def rebuild_address_tables(writer, target_file = None):
    if not target_file:
        target_file = XSLX_FILE
    return ingestion.address.ingest.ingest(target_file, writer)

def rebuild_record_tables(writer, target_file = None):
    if not target_file:
        target_file = JSON_RECORD_FILES
    return ingestion.record.ingest.ingest(target_file, writer)

TARGETS = {
    "ADDRESS" : rebuild_address_tables,
    "RECORD" : rebuild_record_tables
}

def parse_options():
    parser = optparse.OptionParser()
    target_list = ",".join(["\"%s\"" % k for k in  TARGETS.keys()])
    target_help = "Ingest and rebuild tables for TARGET. TARGETs are:\n%s" % (
        target_list)
    mode_list = ("\"UPDATE\" updates existing records (default), " +
                 "\"SKIP\" skips existing records")
    mode_help = "(optional) Specify update mode. Options are: " + mode_list

    parser.add_option("-t", "--target", dest="target",
                      help=target_help,
                      metavar="TARGET")
    parser.add_option("-f", "--file", dest="file",
                      help="(optional) Specify which file to import.",
                      metavar="FILE")
    parser.add_option("-d", "--database", dest="database", default="DEV",
                      help="Database for ingestion, as defined in secrets.py.",
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
    TARGETS[opts.target](db_interface, target_file = opts.file,
                         mode = opts.mode)
