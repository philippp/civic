#!/usr/bin/python
import datetime
import logging
import pprint
import json
import glob
import re
    
"""
Given a source file (or wildcard) for JSON-formatted records, ingest
these to the database."""
def clean(json_record_file, output_dir):
    source_files = glob.glob(json_record_file)
    if len(source_files) == 0:
        logging.error("Found no files in %s.", json_record_file)
        return False

    for source_file in source_files:
        source_filename = source_file.split("/")[-1]
        f = open(source_file, 'r')
        records = json.loads(f.read())
        cleaned_records = []
        for record in records:
            new_record = clean_record(record)
            if new_record:
                cleaned_records.append(new_record)
            else:
                pprint.pprint(record)
        f2 = open(output_dir + "/" + source_filename, "w")
        f2.write(json.dumps(cleaned_records))
        f2.close()
        f.close()
        if len(records) != len(cleaned_records):
            logging.info("Cleaned %s, %d vs %d records",
                         source_file, len(records), len(cleaned_records))
    return True

def find_formatted_value(raw_value, regexp):
    result = re.search(regexp, raw_value)
    if not result:
        return ""
    return result.group(1)

def clean_record(record):
    new_record = dict(record)
    keys_regexps_required = (
        ("apn", "([\d\w]+\-\d+)", False),
        ("date", "(\d{2}\/\d{2}\/\d{4})", True),
        ("id", "(\w{1,2}\d+\-\d{2})", True),
        ("reel_image", "([A-Z]\d{2,4},\d{4})", False),
        ("doctype", "([A-Z_\-\s]+)", True))

    for key, regex, required in keys_regexps_required:
        if getattr(new_record[key], "__iter__", None):
            clean_list = list()
            for i in range(len(new_record[key])):
                val = new_record[key][i]
                clean_val = find_formatted_value(val, regex)
                if clean_val:
                    clean_list.append(clean_val)
                else:
                    continue
            if required and len(clean_list) == 0:
                logging.error(
                    "Record %s contained no valid values for %s",
                    record["id"], key)
                return None
            new_record[key] = clean_list
        else:
            clean_val = find_formatted_value(new_record[key], regex)
            if clean_val:
                new_record[key] = clean_val
            else:
                if required:
                    logging.error("Record %s had an invalid value for %s",
                                  record["id"], key)
                    return None

    if getattr(new_record['date'], "__iter__", None):
        new_record['date'] = new_record['date'][0]

    if getattr(new_record['reel_image'], "__iter__", None):
        if len(new_record['reel_image']) > 0:
            new_record['reel_image'] = new_record['reel_image'][0]
        else:
            new_record['reel_image'] = ""
    return new_record

if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    clean("record_archive/*.json", "deeds")
