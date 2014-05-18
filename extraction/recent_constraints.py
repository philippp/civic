import json
import logging

def fetch(db_interface, start_date):
    # Phase one -- pull Addresses and records.
    query_string = """
SELECT a.address, a.longitude, a.latitude, r.date, r.id 
FROM address a
INNER JOIN (
  SELECT rtbl.block_lot as block_lot, b.record_date as date, b.id  as id
  FROM record_to_block_lot rtbl 
  INNER JOIN record b ON rtbl.record_id = b.id
  WHERE b.record_type=\"NOTICE OF CONSTRAINTS ON REAL PROPERTY\"
  AND b.record_date >= '%s'
  GROUP BY 1 ORDER BY 2 DESC) r
  ON a.block_lot = r.block_lot
GROUP BY 1 ORDER BY 2 DESC
""" % start_date;
    db_interface.cursor.execute(query_string)
    results = db_interface.cursor.fetchall()
    formatted_results = list()
    for result in results:
        formatted_address_result = dict(
            address = result[0],
            date = result[3].strftime("%Y-%m-%d"),
            longitude = result[1],
            latitude = result[2],
            record_id = result[4],)
        formatted_results.append(formatted_address_result)

    # Phase two -- pull owners.
    query_string = """
SELECT r.record_id as record_id, e.best_name
FROM entity e
INNER JOIN (
  SELECT rte.record_id as record_id, rte.entity_id as entity_id
  FROM record_to_entity rte 
  INNER JOIN record r ON rte.record_id = r.id
  WHERE r.record_type=\"NOTICE OF CONSTRAINTS ON REAL PROPERTY\"
  AND rte.relation='grantors' 
  AND r.record_date >= '%s'
  GROUP BY 1 ORDER BY 2 DESC) r
  ON e.id = r.entity_id
GROUP BY 1 ORDER BY 2 DESC
""" % start_date;
    db_interface.cursor.execute(query_string)
    results = db_interface.cursor.fetchall()
    record_name_map = dict()
    for result in results:
        if result[0] not in record_name_map:
            record_name_map[result[0]] = list()
        record_name_map[result[0]].append(result[1])
    for result in formatted_results:
        if result["record_id"] not in record_name_map:
            logging.error("Failed to find grantees for %s", result["record_id"])
            continue
        result["grantees"] = record_name_map[result["record_id"]]
    return formatted_results

def writefile(db_interface, start_date, target_file):
    records = fetch(db_interface, start_date)
    jsonstr = json.dumps(records)
    outfile = open(target_file, "w")
    outfile.write(jsonstr)
    outfile.close()
