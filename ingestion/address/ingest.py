#!/usr/bin/python
import sys
import pprint
import logging
import openpyxl

COLUMN_ORDERING = [
    "addr_n_suffix",
    "addr_num",
    "address",
    "block_lot",
    "cnn",
    "eas_baseid",
    "eas_subid",
    "latitude",
    "longitude",
    "street_name",
    "street_type",
    "unit_num",
    "zipcode"
]

def ingest(source_file, db_interface):
    logging.info("Loading %s, this can take a while..." % source_file)
    workbook = openpyxl.load_workbook(filename = source_file,
                                      use_iterators = True)        
    logging.info("Loaded %s." % source_file)
    header_skipped = False
    rows_written = 0
    for row in workbook.active.iter_rows():
        if not header_skipped:
            header_skipped = True
            continue
        row_values = [c.internal_value for c in row]

        found_data_in_row = False
        for c in row_values:
            if c != None:
                found_data_in_row = True
        if not found_data_in_row:
            logging.info("Empty row encountered at %d, exiting",
                         rows_written + 1)
            break

        try:
            db_interface.write_rows(COLUMN_ORDERING, row_values, "address")
        except Exception, e:
            print str(e)
            pprint.pprint(row)
            sys.exit(2)

        rows_written += 1
        if rows_written % 1000 == 0:
            logging.info("%d rows written", rows_written)
    logging.info("Successfully imported address data.")
