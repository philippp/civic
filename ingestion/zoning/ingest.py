import latlong_lookup
import pdb
import logging
import csv

PAGE_SIZE = 15000

def ingest(db_interface, zoning_filename, blocklot_filename):
    latlonglookup = latlong_lookup.LatLongZoningLookup()
    latlonglookup.initialize(zoning_filename)
    
    blocklot_file = open(blocklot_filename, 'r')
    blocklot_reader = csv.reader(blocklot_file)
    headerrow = dict()
    for row in blocklot_reader:
        if not headerrow:
            for idx in range(len(row)):
                headerrow[row[idx]] = idx
            continue
        blocklot = row[headerrow["MAPBLKLOT"]]
        longitude = float(row[headerrow["longitude"]])
        latitude = float(row[headerrow["latitude"]])

        nearest_zone = latlonglookup.find_nearest(
            latitude, longitude)

        db_interface.update_rows(
            "address",
            update_dict = dict(
                zoning_sim = nearest_zone["ZONING_SIM"]),
            where_dict = dict(
                block_lot = blocklot
                ),
            autocommit = False
            ) 
    db_interface.db.commit()
