import csv
import json
import logging
import pprint
import datetime

col_to_idx = {
    'blocklot' : 0,
    'transfer_count' : 1,
    'earliest_transfer' : 2,
    'latest_transfer' : 3,
    'fastest_transfer_days' : 4,
    'average_transfer_days' : 5,
    'days_since_latest_transfer' : 6
}

PAGE_SIZE = 15000

def fetch(db_interface, target_file=None):
    blockinfos_list = fetch_transfers(db_interface)
    write_rows_to_csv(blockinfos_list, target_file)

def fetch_transfers(db_interface):
    blocklot_timeline = load_blocklot_timeline(db_interface)
    blockinfos = list()
    idx_to_col = dict()
    for k in col_to_idx.keys():
        idx_to_col[col_to_idx[k]] = k
    header = []
    for i in sorted(idx_to_col.keys()):
        header.append(idx_to_col[i])
    blockinfos.append(header)
    for blocklot, timeline in blocklot_timeline.iteritems():
        blockinfo = [''] * len(col_to_idx.keys())
        blockinfo[col_to_idx["blocklot"]] = blocklot
        blockinfo[col_to_idx["transfer_count"]] = len(timeline)
        blockinfo[col_to_idx["earliest_transfer"]] = timeline[-1]
        blockinfo[col_to_idx["latest_transfer"]] = timeline[0]
        fastest_transfer_days = 0
        all_transfer_days = []
        for idx in range(len(timeline)):
            if idx + 1 < len(timeline):
                current_transfer_days = (
                    timeline[idx] - timeline[idx+1]).days
                all_transfer_days.append(current_transfer_days)
                if fastest_transfer_days == 0 or \
                        current_transfer_days < fastest_transfer_days:
                    fastest_transfer_days = current_transfer_days
        blockinfo[col_to_idx["fastest_transfer_days"]] = fastest_transfer_days
        if len(all_transfer_days):
            blockinfo[col_to_idx["average_transfer_days"]] = (
                sum(all_transfer_days) / len(all_transfer_days))
        blockinfo[col_to_idx["days_since_latest_transfer"]] = (
            datetime.date.today() - timeline[0]).days
        blockinfos.append(blockinfo)
    return blockinfos

def load_blocklot_timeline(db_interface):
    offset = 0
    blocklot_timeline = dict()
    while offset >= 0:
        results = db_interface.read_rows(
            ["record_id", "block_lot"],
            "record_to_block_lot",
            limit = offset,
            limit2 = PAGE_SIZE,
            ordergroup = "ORDER BY block_lot DESC")

        # We want to fetch all records for a blocklot, so we read until
        # the last blocklot (which may have gotten snipped).
        logging.info("Fetched %d results.", len(results))
        if len(results) == PAGE_SIZE:
            filtered_results = [r for r in results if r[1] != results[-1][1]]
            offset += len(filtered_results)
            results = filtered_results
        else:
            offset = -1
        record_to_blocklot = dict()
        for r in results:
            record_id = r[0]
            if record_id not in record_to_blocklot:
                # Records can pertain to multiple blocklots. These may be
                # especially interesting, so let's take care to preserve the info.
                record_to_blocklot[record_id] = []
            record_to_blocklot[record_id].append(r[1])

        # Next, fetch all deeds for this page.
        record_ids = record_to_blocklot.keys()
        if not record_ids:
            continue
        record_id_qstring = ",".join(["\"%s\"" % r for r in record_ids])
        query_string = """
SELECT id, record_date, grantees_raw, grantors_raw
FROM record WHERE id IN (%s) ORDER BY record_date DESC
""" % record_id_qstring
        print query_string
        db_interface.cursor.execute(query_string)
        results = db_interface.cursor.fetchall()
        for result in results:
            record_id = result[0]
            action_date = result[1]
            result_blocklots = record_to_blocklot.get(record_id)
            if not result_blocklots:
                logging.error("Failed to find blocklots for %s", record_id)
                continue
            
            for each_blocklot in result_blocklots:
                if each_blocklot not in blocklot_timeline:
                    blocklot_timeline[each_blocklot] = []
                if action_date not in blocklot_timeline:
                    blocklot_timeline[each_blocklot].append(action_date)
    return blocklot_timeline

def write_rows_to_csv(rows, csv_filename):
    with open(csv_filename, 'wb') as csvfile:
        csvwriter = csv.writer(csvfile)
        for row in rows:
            csvwriter.writerow(row)

