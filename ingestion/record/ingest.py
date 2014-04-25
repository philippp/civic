import datetime
import logging
import pprint
import json
import glob
   
"""
Given a source file (or wildcard) for JSON-formatted records, ingest
these to the database."""
def ingest(json_record_file, db_interface):
    source_files = glob.glob(json_record_file)
    if len(source_files) == 0:
        logging.error("Found no files in %s.", json_record_file)
        return False

    for source_file in source_files:
        f = open(source_file, 'r')
        records = json.loads(f.read())
        for record in records:
            ingest_record(record, db_interface)
        f.close()
        logging.info("Ingested %s" % source_file)
    return True

def ingest_record(record, db_interface):
    # Step 1: Find or update a record entry.
    db_record = {
        "id" : record["id"],
        "record_date" : datetime.datetime.strptime(
            record['date'],"%m/%d/%Y").strftime("%Y-%m-%d"),
        "record_type" : record["doctype"],
        "grantees_raw" : ", ".join(["\"%s\"" % g for g in record['grantees']]),
        "grantors_raw" : ", ".join(["\"%s\"" % g for g in record['grantors']]),
        "reel_image" : record["reel_image"]
        }
    colnames = db_record.keys()
    update_cols = list(colnames)
    update_cols.remove("id")
    vals = [db_record[c] for c in colnames]
    db_interface.write_row(colnames, vals, "record", update_cols = update_cols)

    # Step 2: Insert or update record-to-apn mappings.
    for block_lot in record["apn"]:
        db_record = {
            "record_id" : record["id"],
            "block_lot" : block_lot.replace("-","")
        }
        colnames = db_record.keys()
        update_cols = list(colnames)
        update_cols.remove("record_id")
        vals = [db_record[c] for c in colnames]
        db_interface.write_row(colnames, vals, "record_to_block_lot",
                              update_cols = update_cols)

    # Step 3: Insert or update record-to-entity mappings.
    for relation in ['grantees', 'grantors']:
        for entity_alias in record[relation]:
            # Do we already know who is behind this alias?
            response = db_interface.read_rows(
                ['alias_name', 'entity_id'],
                'alias_to_entity',
                alias_name = entity_alias)
            if not response:
                # If we have't seen the alias before, create it and a new
                # entity.
                entity_id = db_interface.write_row(
                    ["best_name"], [entity_alias], "entity")
                db_interface.write_row(
                    ["entity_id", "alias_name"], [entity_id, entity_alias],
                    "alias_to_entity")
            else:
                # We make the assumption that an alias can only point to one
                # org. This is a fallacy, since common names are common and will
                # overlap. TODO(philippp): Fix this.
                entity_id = response[0][1]

            db_interface.write_row(
                ['record_id', 'entity_id', 'relation'],
                [record["id"], entity_id, relation],
                'record_to_entity',
                ['record_id'])

