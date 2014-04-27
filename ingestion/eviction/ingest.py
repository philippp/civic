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

def is_number(s):
    try:
        float(s)
        return True
    except ValueError:
        return False

def ingest_ellis_csv(source_file, db_interface):
    csv_file = open(source_file, 'r')
    csvreader = csv.reader(csv_file)
    rowidx = 0
    # CSV Cols:
    # date_filed,petition,address,units,zip,landlord_name,senior_disabled
    target_cols = ['date_filed', 'petition', 'address', 'units',
                   'landlord_names', 'senior_disabled', 'eviction_type']
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
