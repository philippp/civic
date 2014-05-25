import logging
import pprint
import re

class AddressLookup:
    
    def __init__(self):
        self.street_names = set()
        self.street_types = set()
        self.unit_numbers = set()

    def initialize(self, db_interface):
        self.db_interface = db_interface
        self.db_interface.cursor.execute(
            "SELECT DISTINCT street_name FROM address")
        raw_street_names = self.db_interface.cursor.fetchall()
        for street_name_db_row in raw_street_names:
            street_name = street_name_db_row[0]
            # Aliasing rule: 04TH == 4TH
            if street_name[0] == "0":
                street_name = street_name[1:]
            self.street_names.add(street_name)

        self.db_interface.cursor.execute(
            "SELECT DISTINCT street_type FROM address")
        raw_street_types = self.db_interface.cursor.fetchall()
        for street_type_db_row in raw_street_types:
            self.street_types.add(street_type_db_row[0])

        self.db_interface.cursor.execute(
            "SELECT DISTINCT unit_num FROM address")
        raw_unit_numbers = self.db_interface.cursor.fetchall()
        for unit_num_db_row in raw_unit_numbers:
            unit_number = unit_num_db_row[0]
            self.unit_numbers.add(unit_number)

    """Parse an address string and look up metadata from the database.
    Returns a list of matching addresses, or an empty list if nothing was
    matched. Supported address formats:
    2-6 Main St
    729D Long Street Name Apt #2
    etc
    """
    def lookup(self, address_str):
        return self.lookup_parsed(self.parse(address_str))

    def parse(self, address_str):
        address_tokens = [t.upper() for t in re.findall(
                "([\d\w\-]+)", address_str)]
        address_tokens = filter(lambda t: t not in JUNK_TOKENS, address_tokens)

        # Some addresses spell out the number.
        if address_tokens[0] in NUMBERS:
            address_token[0] = NUMBERS[address_token[0]]

        # The token index of the street name varies based on how the street number
        # is specified (ex: 14D vs 14 D).
        street_name_idx = 0

        # The token index of the (possible) apartment number varies on whether the
        # street had a street type.
        unit_number_idx = -1;

        # Index of the current token index for (multi-)address number parsing.
        cur_addr_num_idx = 0

        # Sometimes a single address line means several addresses.
        # Ex: 10-12 Main St
        addresses = []
        while True:
            addr_num_match = re.match("^([0-9\\-]+)([A-Z]?)$",
                                      address_tokens[cur_addr_num_idx])
            if not addr_num_match:
                break

            cur_addr_num_idx += 1
            street_name_idx += 1

            address = dict()
            address["addr_num"] = addr_num_match.groups()[0]
            if addr_num_match.groups()[1]:
                address["addr_n_suffix"] = addr_num_match.groups()[1]

            if len(address_tokens[cur_addr_num_idx]) == 1:
                # If the post-incremented addr index is a suffix, we consume that
                # token and skip ahead once more.
                address["addr_n_suffix"] = address_tokens[cur_addr_num_idx]
                cur_addr_num_idx += 1
                street_name_idx += 1

            # We populate the address array with placeholder entries for each
            # house number and populate the street info afterwards.
            addresses.append(address)

            # We also want to interpret ranges of numbers for multi-unit
            # buildings.
            numbers = address["addr_num"].split("-")
            if len(numbers) == 2:
                addresses[0]["addr_num"] = numbers[0]
                start_num = int(numbers[0])
                end_num = int(numbers[1])
                # Consecutive units are on the same side of the street
                for house_num in range(start_num + 2, end_num + 1, 2):
                    addresses.append(dict(addr_num = str(house_num)))
        if not addresses:
            logging.error("No address number found")
            return []
        # Street names do not have numbers in the second word of the street.
        # Pick up N-tuples of tokens after the address-number, starting from
        # the entire rest of the address and then getting smaller until we
        # encounter something non-alphabetical.
        street_name_end_idx = len(address_tokens)
        while street_name_end_idx >= street_name_idx:
            street_name = " ".join(
                address_tokens[street_name_idx:street_name_end_idx])
            if street_name in self.street_names:
                [a.__setitem__("street_name", street_name) for a in addresses]
                unit_number_idx = street_name_end_idx
                break
            street_name_end_idx -= 1
        if "street_name" not in addresses[0]:
            logging.error("No street name found")
            return []
        
        # Try to find a street type (Ave, St, etc) -- there may not be one!
        for token in address_tokens[street_name_end_idx:]:
            if token in STREET_TYPES_ALIASES:
                token = STREET_TYPES_ALIASES[token]
            if token in self.street_types:
                [a.__setitem__("street_type", token) for a in addresses]
                unit_number_idx += 1
                break

        # Finally, we want to check for an apartment number, which again may
        # not exist. We do not expect address number ranges AND apartment numbers.
        address_unit_numbers = []
        if unit_number_idx > 0 and unit_number_idx < len(address_tokens):
            for unit_number in address_tokens[unit_number_idx:]:
                if unit_number in self.unit_numbers:
                    address_unit_numbers.append(unit_number)

            if len(addresses) == 1:
                new_addresses = []
                for address_unit_number in address_unit_numbers:
                    address = dict(addresses[0])
                    address["unit_num"] = address_unit_number
                    new_addresses.append(address)
                addresses = new_addresses
        return addresses

    """Given a list of parsed addresses, look up known addresses in SF.
    Populates all known metadata about the address."""
    def lookup_parsed(self, parsed_address_list):
        if not parsed_address_list:
            return []
        query_kwargs = dict()
        for address in parsed_address_list:
            for k, v in address.iteritems():
                if k in ["unit_num"]:
                    continue
                if k not in query_kwargs:
                    query_kwargs[k] = set()
                query_kwargs[k].add(str(v))
        for k in query_kwargs.keys():
            query_kwargs[k] = list(query_kwargs[k])

        rows = self.db_interface.read_rows(
            ["addr_n_suffix", "addr_num", "block_lot",
             "longitude", "latitude", "street_name",
             "street_type", "unit_num", "zipcode"],
            "address",
            **query_kwargs)

        matched_address_list = []
        for parsed_address in parsed_address_list:
            for row in rows:
                address = dict(parsed_address)
                if (row[1] == address["addr_num"] and \
                        row[5] == address["street_name"] and \
                        (("street_type" not in address) or 
                         (address["street_type"] == row[6])) and \
                        (("addr_n_suffix" not in address) or
                         (address["addr_n_suffix"] == row[0]))):
                    address["block_lot"] = row[2]
                    address["longitude"] = row[3]
                    address["latitude"] = row[4]
                    address["zipcode"] = row[8]
                    address["unit_num"] = row[7]
                    if address["block_lot"] != None:
                        matched_address_list.append(address)
        return matched_address_list

JUNK_TOKENS = {
    "APT",
    "UNIT"
}

NUMBERS = {
    "ONE" : "1",
    "TWO" : "2",
    "THREE" : "3",
    "FOUR" : "4",
    "FIVE" : "5"
}

STREET_TYPES_ALIASES = {
  "STREET" : "ST",
  "COURT" : "CT",
  "AVENUE" : "AVE",
  "BOULEVARD" : "BLVD",
  "TERRACE" : "TER",
  "LANE" : "LN",
  "DRIVE" : "DR",
  "ALLEY" : "ALY",
  "PLACE" : "PL",
  "CIRCLE" : "CIR",
  "ROAD" : "RD",
  "PLAZA" : "PLZ",
  "HIGHWAY" : "HWY",
  "STREETWAY" : "STWY",
  "HILL" : "HL"
}
