#include "digestion/locations/address_lookup.h"
#include <iostream>

using std::string;

int main(int argc, char* argv[]) {
  AddressLookup al;
  al.Initialize();
  if (argc < 2) {
    std::cout << "Usage: " << std::endl;
    std::cout << argv[0] << " \"ADDRESS\"" << std::endl;
    return -1;
  }
  vector<Address> addresses;
  al.Parse(string(argv[1]), &addresses);
  for (size_t i = 0; i < addresses.size(); ++i) {
    const Address& address = addresses[i];
    std::cout << "[[ Address " << i << " ]]" << std::endl;
    for (const auto& iter : address) {
      std::cout << iter.first << ": " << iter.second << std::endl;
    }
  }

  return 0;
} 
