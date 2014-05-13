# Resolve eviction addresses using the address lookup module. This script requires
# the address lookup module to have been compiled.

import urllib2
import urllib
import json
import time
import pprint
import addresslookup
import logging

looker = addresslookup.AddressLookup()

def augment_evictions(db_interface):
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
            if address_string in ("1201Guerrero St 1", "P.O Box 42"):
                # TODO: This should not segfault.
                continue
            matching_addresses = looker.lookup(address_string)
            if not matching_addresses:
                logging.error("Failed to resolve address: %s", address_string)
                failed_to_resolve.add(address_string)
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
    
    failed_to_resolve = list(failed_to_resolve)
    logging.info("Failed to resolve %d addresses.", len(failed_to_resolve))
