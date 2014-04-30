#include <stdlib.h>
#include <string>
#include <vector>
#include <unordered_map>
#include <memory>
#include <mysql++/mysql++.h>

using std::vector;
using std::unordered_map;
using std::unique_ptr;
using std::string;

typedef unordered_map<string, string> Address;

// Address lookup and resolution module. Takes a string and returns up to N matching
// addresses, including APN and other info.
class AddressLookup {
public:
  // Parses an address string that represents one or more addresses, populates
  // a vector with all matching addresses.
  void Parse(const string& address_str, vector<Address>* address) const;
  // Loads suffixes and street names into memory.
  int Initialize();

private:
  unique_ptr<mysqlpp::Connection> connection_;

  // All recognized street names in San Francisco, including aliases. For
  // example, 04TH is also 4TH, etc.
  // The keys are all recognized street names, and the values are the
  // canonical entries.
  unordered_map<string, string> street_names_;
  
  // All recognized street types in San Francisco. Ex: AVE, ST, LN. Also
  // contains aliases (ex: TERRACE for TER).
  unordered_map<string, string> street_types_;

  // All unit numbers in San Francisco.
  unordered_map<string, string> unit_numbers_;
};
