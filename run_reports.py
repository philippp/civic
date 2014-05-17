#!/usr/bin/python
import logging
import optparse
import pprint
import sys
import datetime

import db.interface
from extraction import recent_constraints

def extract_recent_constraints(db_interface,
                               target_file="",
                               start_date=""):
    start_date = start_date or \
        (datetime.date.today() -
         datetime.timedelta(days=60)).strftime(
            "%Y-%m-%d")
    recent_constraints.writefile(db_interface,
                                 start_date,
                                 target_file)

REPORTS = {
    "RECENT_CONSTRAINTS" : extract_recent_constraints,
}

def parse_options():
    parser = optparse.OptionParser()
    report_list = ",".join(["\"%s\"" % k for k in REPORTS.keys()])
    report_help = "Extract report. Reports are:\n%s" % (
        report_list)
    parser.add_option("-r", "--report", dest="report",
                      help=report_help,
                      metavar="REPORT")
    parser.add_option("-f", "--file", dest="file",
                      help="Specify file to write to.",
                      metavar="FILE")
    parser.add_option("-s", "--start_date", dest="start_date",
                      help="Specify start date of report in YYYY-MM-DD " + \
                          " format. Default is 180 days ago.",
                      metavar="STARTDATE")
    parser.add_option("-d", "--database", dest="database", default="DEV",
                      help="Database for ingestion, as defined in secrets.py.",
                      metavar="DATABASE")
    
    opts, args = parser.parse_args()
    if opts.report not in REPORTS.keys():
        parser.print_help()
        sys.exit(2)
    return opts

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    opts = parse_options()
    db_interface = db.interface.DBInterface()
    db_interface.initialize(opts.database)
    REPORTS[opts.report](db_interface, target_file = opts.file, )
