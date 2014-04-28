#!/usr/bin/python
import csv
import logging
import pprint
import sys
import xlrd

def ingest_ellis(source_files, db_interface):
    for source_file in source_files:
        if source_file.endswith(".xls"):
            ingest_ellis_xls(source_file, db_interface)
        elif source_file.endswith(".csv"):
            ingest_ellis_csv(source_file, db_interface)

def ingest_omi(source_files, db_interface):
    for source_file in source_files:
        if source_file.endswith(".csv"):
            ingest_omi_csv(source_file, db_interface)

def is_number(s):
    try:
        float(s)
        return True
    except ValueError:
        return False

def ingest_omi_csv(source_file, db_interface):
    csv_file = open(source_file, 'r')
    csvreader = csv.reader(csv_file)
    rowidx = 0
    # CSV Cols:
    # petition,Date,address_1,Apt,City,CA,Zip,Type,yearly count
    target_cols = ['petition', 'date_filed', 'address', 'eviction_type']
    page_size = 100
    rows = list()
    for row in csvreader:
        if rowidx == 0:
            # Skip the header row
            rowidx += 1
            continue

        if len(row) < 7:
            # Skip too-short rows (e.g. newline at end of file)
            logging.error("Encountered too-short row: %s", str(row))
            continue

        if not row[1]:
            # Some rows don't have dates. We drop records on properties
            # by ignoring these, but it allows us to rely on having dates
            # elsewhere.
            logging.error("Encountered record without a date: %s",
                          str(row))
            continue

        date_parts = row[1].split('/')
        if len(date_parts) != 3 or len(date_parts[2]) != 4:
            logging.error("Malformed date: %s expected MM/DD/YYYY", row[1])
            print row
            sys.exit(2)
        if row[7] != "OMI":
            logging.warning("Expected OMI in cell 7: %s", str(row))
        rows.append([
                row[0], # petition
                "%s-%s-%s" % (date_parts[2], date_parts[0], date_parts[1]),
                row[2], # address
                'omi' # eviction_type
                ])
        if len(rows) > page_size:
            db_interface.write_row(
                target_cols, rows, 'eviction', update_cols = target_cols)
            rows = list()
    db_interface.write_row(
        target_cols, rows, 'eviction', update_cols = target_cols)

"""
Ingest historical ellis act data in xls format, as provided by the San Francisco
Tenant's Union. See: http://sftu.org/vcsites.html
"""
def ingest_ellis_xls(source_file, db_interface):
    workbook = xlrd.open_workbook(filename = source_file)
    sheet = workbook.sheet_by_name("Sheet1")
    rowidx = 0
    end_reached = False
    update_page_size = 3
    update_page = list()
    target_cols = ['date_filed', 'petition', 'landlord_names', 'address',
                   'eviction_type']
    while rowidx < sheet.nrows:
        if rowidx == 0:
            rowidx += 1
            continue
        cur_row = sheet.row(rowidx)
        xld = xlrd.xldate_as_tuple(cur_row[0].value, 0)
        datestr = "%s-%s-%s" % (xld[0], xld[1], xld[2])
        update_page.append([
                datestr,
                cur_row[1].value,
                cur_row[2].value,
                "%s %s %s" % (
                    cur_row[3].value, cur_row[4].value, cur_row[5].value),
                'ellis'
                ])
        if len(update_page) == update_page_size:
            db_interface.write_row(
                target_cols, update_page, 'eviction', update_cols = target_cols)
            update_page = list()
        rowidx += 1

    db_interface.write_row(
        target_cols, update_page, 'eviction', update_cols = target_cols)
    update_page = list()

def ingest_ellis_csv(source_file, db_interface):
    csv_file = open(source_file, 'r')
    csvreader = csv.reader(csv_file)
    rowidx = 0
    # CSV Cols:
    # date_filed,petition,address,units,zip,landlord_name,senior_disabled
    target_cols = ['date_filed', 'petition', 'address', 'unit_count',
                   'landlord_names', 'senior_disabled_count', 'eviction_type']
    page_size = 100
    rows = list()
    for row in csvreader:
        if rowidx == 0:
            # Skip the header row
            rowidx += 1
            continue

        date_parts = row[0].split('/')
        if len(date_parts) != 3:
            logging.error("Malformed date: %s", row[0])
            sys.exit(2)
        year = ""
        if int(date_parts[-1]) < 50:
            year = "20" + date_parts[-1]
        else:
            year = "19" + date_parts[-1]
        rows.append([
                "%s-%s-%s" % (year, date_parts[0], date_parts[1]),
                row[1], # petition
                row[2], # address
                is_number(row[3]) and row[3] or None, # units
                row[5], # landlord_names
                is_number(row[6]) and row[6] or None, # senior_disabled
                'ellis' # eviction_type
                ])
        if len(rows) > page_size:
            db_interface.write_row(
                target_cols, rows, 'eviction', update_cols = target_cols)
            rows = list()
    db_interface.write_row(
        target_cols, rows, 'eviction', update_cols = target_cols)
