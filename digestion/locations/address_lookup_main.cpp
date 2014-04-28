#include "digestion/locations/address_lookup.h"
#include <iostream>

using std::string;

int main(int argc, char* argv[]) {
  std::cout << "Starting!\n";
  AddressLookup al;
  al.Initialize();
  vector<Address> addresses;
  string parse_str = "760 14TH ST APT#1";
  if (argc > 1) {
    parse_str = string(argv[1]);
  }
  al.Parse(parse_str, &addresses);
  std::cout << "Done!\n";
} 
