import csv
from libraries import kdtree
import logging

PAGE_SIZE = 20000

class LatLongZoningLookup:
    def initialize(self, zoning_csv):
        self.my_tree = kdtree.create(dimensions=2)
        self.headerrow = dict()
        self.latlong_to_zoning = dict()
        with open(zoning_csv, 'r') as csvfile:
            csvreader = csv.reader(csvfile)
            for row in csvreader:
                if not self.headerrow:
                    for idx in range(len(row)):
                        self.headerrow[row[idx]] = idx
                    continue

                if row[self.headerrow['ZONING_SIM']] == 'P':
                    # There's a bug that causes public land to mark
                    # neighboring buildings as public land as well.
                    continue

                latitude = float(row[self.headerrow['latitude']])
                longitude = float(row[self.headerrow['longitude']])
                self.my_tree.add([latitude, longitude])
                self.latlong_to_zoning["%10f|%10f" % (
                        latitude, longitude)] = list(row)
        self.my_tree.rebalance()

    def find_nearest(self, latitude, longitude):
        neighbor = self.my_tree.search_nn([latitude, longitude])
        return dict(zip(self.headerrow, self.latlong_to_zoning["%10f|%10f" % (
                neighbor[0].data[0], neighbor[0].data[1])]))

class LatLongLookup:
    def initialize(self, db_interface):
        # Load blocklot LatLong locations from the database.
        self.my_tree = kdtree.create(dimensions=2)
        self.latlong_to_blocklot = dict()
        offset = 0
        while offset >= 0:
            results = db_interface.read_rows(
                ["block_lot", "latitude", "longitude"],
                "address",
                limit = offset,
                limit2 = PAGE_SIZE,
                ordergroup = "ORDER BY block_lot DESC")

            # We want to fetch all records for a blocklot, so we read until
            # the last blocklot (which may have gotten snipped).
            logging.info("Fetched %d results.", len(results))
            if len(results) == PAGE_SIZE:
                filtered_results = [r for r in results if r[0] != results[-1][0]]
                offset += len(filtered_results)
                results = filtered_results
            else:
                offset = -1
            if not results:
                continue
            points = []
            for i in range(len(results)):
                if i != 0:
                    if results[i][1] == results[i-1][1] and \
                            results[i][2] == results[i-1][2]:
                        continue
                
                self.my_tree.add((results[i][1], results[i][2]))
                points.append((results[i][1], results[i][2]))
                self.latlong_to_blocklot["%10f|%10f" % (
                        results[i][1], results[i][2])] = results[i][0]
            self.my_tree.rebalance()

    def find(self, latitude, longitude):
        pass
