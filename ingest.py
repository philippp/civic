#!/usr/bin/python
import logging
import optparse
import pprint
import sys

import db.interface
import ingestion.address.ingest
import ingestion.record.ingest
import ingestion.eviction.ingest
import digestion.locations.resolve_eviction_addresses

ADDRESS_FILE = 'data/address/EAS_address_with_blklot.xlsx'
RECORD_FILES = [
    'data/record/deed/*.json',
    'data/record/deed_nontax/*.json',
    'data/record/deed_of_trust/*.json',
    'data/record/deed_of_trust_with_rents/*.json',
    'data/record/notice_of_constraints_on_real_property/*.json'
    ]
    
EVICTION_FILES = {
    'ellis' : [
        'data/eviction/SF Ellis Petition List 1997-March 13.xls',
        'data/eviction/kelsey_ellis_most_recent.csv'],
    'omi' : [
        'data/eviction/kelsey_omi_most_recent.csv']
}

def rebuild_address_tables(writer, target_file = None):
    if not target_file:
        target_file = ADDRESS_FILE
    return ingestion.address.ingest.ingest(target_file, writer)

def rebuild_record_tables(writer, target_file = None):
    if not target_file:
        target_file = RECORD_FILES
    success = True
    if hasattr(target_file, "__iter__"):
        for target_file_ in target_file:
            success = success and ingestion.record.ingest.ingest(
                target_file_, writer)
    else:
        success = ingestion.record.ingest.ingest(
            target_file, writer)
    return success

def rebuild_eviction_tables(writer, target_file = None):
    if not target_file:
        target_file = EVICTION_FILES
    ingestion.eviction.ingest.ingest_ellis(target_file['ellis'], writer)
    ingestion.eviction.ingest.ingest_omi(target_file['omi'], writer)

# Requires eviction and address tables to have been built AND for the address
# resolution service to be running.
def digest_eviction_tables(writer, target_file = None):
    digestion.locations.resolve_eviction_addresses.augment_evictions(writer)

TARGETS = {
    "ADDRESS" : rebuild_address_tables,
    "RECORD" : rebuild_record_tables,
    "EVICTION" : rebuild_eviction_tables,
    "DIGEST_EVICTION" : digest_eviction_tables
}

def parse_options():
    parser = optparse.OptionParser()
    target_list = ",".join(["\"%s\"" % k for k in  TARGETS.keys()])
    target_help = "Ingest and rebuild tables for TARGET. TARGETs are:\n%s" % (
        target_list)
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
    TARGETS[opts.target](db_interface, target_file = opts.file)
