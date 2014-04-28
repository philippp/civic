#include "digestion/locations/address_lookup.h"
#include <boost/algorithm/string/predicate.hpp>
#include <boost/regex.hpp>
#include <boost/algorithm/string.hpp>

using mysqlpp::Query;
using mysqlpp::StoreQueryResult;

void string_split(const std::string &s, char delim,
		  std::vector<std::string>* elems) {
  std::stringstream ss(s);
  std::string item;
  while (std::getline(ss, item, delim)) {
    elems->push_back(item);
  }
}


void AddressLookup::Parse(const string& address_str,
			  vector<Address>* addresses) const {
  std::cout << "Interpreting address_str" << address_str << std::endl;

  // Tokenize the address.
  vector<string> address_tokens;
  boost::regex regex("([a-zA-Z0-9\\-]+)");
  boost::match_results<std::string::const_iterator> results;
  string::const_iterator start = address_str.begin();
  string::const_iterator end = address_str.end();
  while (boost::regex_search(start, end, results, regex)) {
    string token(results[1].first, results[1].second);
    boost::to_upper(token);
    address_tokens.push_back(token);
    start = results[0].second;
  }
  Address address;
  int street_type_idx = -1;
  int street_name_idx = -1;
  int street_number_idx = -1;

  // Strategy 1: Try to find a street type to anchor off of.
  for (const string& token : address_tokens) {
    if (street_types_.find(token) != street_types_.end()) {
      address["street_type"] = token;
      std::cout << "Found street type: " << token << std::endl;
    }
  }
  boost::match_results<std::string::const_iterator> address_num_result;
  boost::regex address_num_regex("([0-9\\-]+)([A-Z]?)");
  if (boost::regex_match(address_tokens[0], address_num_result,
			 address_num_regex)) {
    string token(address_num_result[1].first, address_num_result[1].second);
    address["add_num"] = token;
    std::cout << "Found token: " << token << std::endl;
    string addr_n_suffix(address_num_result[2].first,
			 address_num_result[2].second);
    if (addr_n_suffix.length() != 0) {
      address["addr_n_suffix"] = addr_n_suffix;
    }
    else if (address_tokens[1].length() == 1) {
      address["addr_n_suffix"] = address_tokens[1];
    }
    std::cout << "Suffix: " << address["addr_n_suffix"] << std::endl;
  }
  
}

int AddressLookup::Initialize() {
  connection_.reset(new mysqlpp::Connection(false));
  connection_->connect("sfabuse", "localhost", "root", "");
  Query query = connection_->query();
  query << "SELECT DISTINCT street_name FROM address";
  StoreQueryResult result = query.store();
  for (size_t i = 0; i < result.num_rows(); i++) {
    string street_name;
    result[i]["street_name"].to_string(street_name);
    street_names_[street_name] = street_name;
    // Aliasing rule: 04TH == 4TH
    if (boost::starts_with(street_name, "0")) {
      street_names_[street_name.substr(1)] = street_name;
    }
  }

  // Load street types.
  query = connection_->query();
  query << "SELECT DISTINCT street_type FROM address";
  result = query.store();
  for (size_t i = 0; i < result.num_rows(); i++) {
    string street_type;
    result[i]["street_type"].to_string(street_type);
    street_types_[street_type] = street_type;
  }
  street_types_["STREET"] = "ST";
  street_types_["COURT"] = "CT";
  street_types_["AVENUE"] = "AVE";
  street_types_["BOULEVARD"] = "BLVD";
  street_types_["TERRACE"] = "TER";
  street_types_["LANE"] = "LN";
  street_types_["DRIVE"] = "DR";
  street_types_["ALLEY"] = "ALY";
  street_types_["PLACE"] = "PL";
  street_types_["CIRCLE"] = "CIR";
  street_types_["ROAD"] = "RD";
  street_types_["PLAZA"] = "PLZ";
  street_types_["HIGHWAY"] = "HWY";
  street_types_["STREETWAY"] = "STWY";
  street_types_["HILL"] = "HL";

  // Load unit numbers.
  query = connection_->query();
  query << "SELECT DISTINCT unit_num FROM address";
  result = query.store();
  for (size_t i = 0; i < result.num_rows(); i++) {
    string unit_num;
    result[i]["unit_num"].to_string(unit_num);
    street_names_[unit_num] = unit_num;
  }

  return 0;
}
