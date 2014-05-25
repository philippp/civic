# Resolve eviction addresses using the address lookup module. This script requires
# the address lookup module to have been compiled.

import urllib2
import urllib
import json
import time
import pprint
import logging
import address_lookup

def augment_evictions(db_interface, failed_address_filename=""):
    failed_address_file = None
    if failed_address_filename:
        failed_address_file = open(failed_address_filename, 'w')
    looker = address_lookup.AddressLookup()
    looker.initialize(db_interface)

    page_size = 100
    idx = 0
    failed_to_resolve = set()
    while True:
        rows = db_interface.read_rows(
            ["address", "petition"], "eviction", idx, page_size)
        idx += len(rows)
        if not rows:
            logging.info("Finished reading after %d rows of eviction data.",
                         idx)
            break

        petition_blocklots_map = dict()
        for row in rows:
            # We don't expect unicode in 'murrican addresses, so drop all
            # non-ascii to deal with dirrty input data.
            address_string = unicode(row[0]).encode('ascii', 'ignore')
            if not len(row) == 2:
                logging.error("Surprised at row: %s", str(row))
                continue
            logging.info("Looking up: \"%s\"", address_string)
            matching_addresses = looker.lookup(address_string)
            import pprint
            pprint.pprint(matching_addresses)

            if not matching_addresses:
                logging.error("Failed to resolve address: %s", address_string)
                if failed_address_file:
                    failed_address_file.write("%s\n" % address_string)
                continue

            eviction_blocklots = set()
            for address in matching_addresses:
                eviction_blocklots.add(address["block_lot"])
            petition_blocklots_map[row[1]] = list(eviction_blocklots)
        db_map_rows = []
        for petition, blocklots in petition_blocklots_map.iteritems():
            for blocklot in blocklots:
                db_map_rows.append([petition, blocklot])
        db_interface.write_rows(["petition", "block_lot"], db_map_rows,
                                "eviction_to_blocklot", ["block_lot"])
        logging.info("Wrote %d rows", len(db_map_rows))
        db_map_rows = []
    
    if failed_address_file:
        failed_address_file.close()
    logging.info("Failed to resolve %d addresses.", len(failed_to_resolve))
