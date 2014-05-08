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
    batch_size = 500
    rows_to_write = []
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
        rows_to_write.append(row_values)
        if len(rows_to_write) % batch_size == 0:
            db_interface.write_rows(COLUMN_ORDERING,
                                    rows_to_write, "address")
            rows_to_write = []
            rows_written += batch_size
            logging.info("%d rows written", rows_written)

    db_interface.write_rows(COLUMN_ORDERING,
                            rows_to_write, "address")
    logging.info("Successfully imported address data.")
