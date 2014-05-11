#include "digestion/locations/address_lookup.h"
#include <iostream>
#include <stdlib.h>
#include <jsoncpp/json/writer.h>
#include <unordered_map>

using std::string;

// http://stackoverflow.com/questions/8447308/ctypes-and-string

extern "C" {
  AddressLookup* AddressLookup_new() {
    AddressLookup* lookup = new AddressLookup();
    lookup->Initialize();
    return lookup;
  }

  // Takes an address string and returns a JSON-encoded dictionary
  // of address components.
  char* LookupAddress(AddressLookup* lookup, char* address_string) {
    vector<Address> raw_addresses;
    vector<Address> resolved_addresses;
    lookup->Parse(address_string, &raw_addresses);
    lookup->LookupByAddresses(raw_addresses, &resolved_addresses);
    Json::Value root;   // will contains the root value after parsing
    for (size_t i = 0; i < resolved_addresses.size(); ++i) {
      Json::Value address_value;
      const Address& resolved_address = resolved_addresses[i];
      for (const auto& iter : resolved_address) {
	address_value[iter.first] = iter.second;
      }
      root.append(address_value);
    }
    Json::FastWriter writer = Json::FastWriter();
    string json_addresses = writer.write(root);
    size_t char_count = (json_addresses.length()+1);
    char* toret = static_cast<char*>(malloc(char_count * sizeof(char)));
    memcpy(toret, json_addresses.c_str(), char_count);
    return toret;
  }

  void FreeAddressJSON(char* json_addresses) {
    free(json_addresses);
  }
}
