import datetime
import logging
import pprint
import json
import glob
import time
import sys

"""
Given a source file (or wildcard) for JSON-formatted records, ingest
these to the database."""
def ingest(json_record_file, db_interface, skip_existing = False):
    source_files = glob.glob(json_record_file)
    source_files.sort()
    if len(source_files) == 0:
        logging.error("Found no files in %s.", json_record_file)
        return False

    for source_file in source_files:
        t_start = time.time()
        f = open(source_file, 'r')
        records = json.loads(f.read())
        f.close()
        ingest_records(records, db_interface)
        logging.info("Ingested %s with %d records in %2.2f seconds",
                     source_file, len(records), time.time() - t_start)
    return True

def ingest_records(records, db_interface):
    if len(records) == 0:
        return  # If the list of records is empty, we're done.

    # Step 1: Insert or update all top-level records.
    db_records = []
    for record in records:
        # Some aliases contain backslashes, which break SQL insertion.
        # TODO: figure out why, deal with backslashes.
        scrub = lambda s: s.replace("\\","").strip()
        record['grantees'] = [scrub(g) for g in record['grantees']]
        record['grantors'] = [scrub(g) for g in record['grantors']]
        grantees_raw = ", ".join(["\"%s\"" % g for g in record['grantees']])
        grantors_raw = ", ".join(["\"%s\"" % g for g in record['grantors']])

        db_records.append({
            "id" : record["id"],
            "record_date" : datetime.datetime.strptime(
                record['date'],"%m/%d/%Y").strftime("%Y-%m-%d"),
            "record_type" : record["doctype"],
            "grantees_raw" : grantees_raw,
            "grantors_raw" : grantors_raw,
            "reel_image" : record["reel_image"]
            })
    colnames = db_records[0].keys()
    update_cols = list(colnames)
    update_cols.remove("id")
    vals = []
    for db_record in db_records:
        vals.append([db_record[c] for c in colnames])

    db_interface.write_row(
        colnames, vals, "record", update_cols = update_cols, autocommit = False)

    # Step 2: Write all APNs (blocks and lots) for these records.
    db_records = []
    for record in records:
        for block_lot in record["apn"]:
            db_records.append({
                    "record_id" : record["id"],
                    "block_lot" : block_lot.replace("-","")
                    })
    if len(db_records) > 0:
        colnames = db_records[0].keys()
        update_cols = list(colnames)
        update_cols.remove("record_id")
        vals = []
        for db_record in db_records:
            vals.append([db_record[c] for c in colnames])
        db_interface.write_row(colnames, vals, "record_to_block_lot",
                               update_cols = update_cols,
                               autocommit = False)

    # Step 3: Resolve all aliases. Create entries for new aliases and
    #         associate entities with record.
    entity_aliases = []
    for record in records:
        for relation in ['grantees', 'grantors']:
            entity_aliases += record[relation]
    entity_aliases = list(set(entity_aliases))
    alias_entity_map = dict()
    if len(entity_aliases) == 0:
        return

    # Step 3.a: Which aliases do we already know?
    response = db_interface.read_rows(
        ['alias_name', 'entity_id'],
        'alias_to_entity',
        alias_name = entity_aliases)
    for row in response:
        alias_entity_map[row[0]] = row[1]

    # Step 3.b: For the aliases new to us, we must create entities.
    new_aliases = list(set(entity_aliases) - set(alias_entity_map.keys()))
    response = db_interface.write_row(
        ['best_name'],
        new_aliases,
        'entity')

    # Step 3.c: Now fetch the newly created entity IDs and augment our map.
    response = db_interface.read_rows(
        ['best_name', 'id'],
        'entity',
        best_name = entity_aliases)
    for row in response:
        alias_entity_map[row[0]] = row[1]

    if len(alias_entity_map.keys()) != len(entity_aliases):
        print "WE'RE GOING DOWN!!!!"
        if len(alias_entity_map.keys()) > len(entity_aliases):
            print "More keys in the map than we had entries in the list"
            pprint.pprint(set(alias_entity_map.keys()) - set(entity_aliases))
        else:
            print "Bigger list than we had keys in the map"
            pprint.pprint(set(entity_aliases) - set(alias_entity_map.keys()))
        print "time to die."
        sys.exit(2)

    # Step 3.d: For the aliases new to us, we also need alias-entity maps in DB.
    new_alias_rows = list()
    for alias in new_aliases:
        new_alias_rows.append([alias, alias_entity_map[alias]])
    response = db_interface.write_row(
        ['alias_name', 'entity_id'],
        new_alias_rows,
        'alias_to_entity')

    # Step 3.e: Finally update the record_to_entity mapping.
    record_to_entity_list = list()
    for record in records:
        for relation in ['grantees', 'grantors']:
            for alias in record[relation]:
                record_to_entity_list.append(
                    [record['id'], alias_entity_map[alias], relation])
    db_interface.write_row(
        ["record_id", "entity_id", "relation"], record_to_entity_list,
        "record_to_entity", ["record_id", "entity_id", "relation"])
