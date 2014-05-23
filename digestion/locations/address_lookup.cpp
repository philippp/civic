#include "digestion/locations/address_lookup.h"
#include <boost/algorithm/string/predicate.hpp>
#include <boost/regex.hpp>
#include <boost/algorithm/string.hpp>

using mysqlpp::Query;
using mysqlpp::StoreQueryResult;

namespace {
  void string_split(const std::string &s, char delim,
		    std::vector<std::string>* elems) {
    std::stringstream ss(s);
    std::string item;
    while (std::getline(ss, item, delim)) {
      elems->push_back(item);
    }
  }

  // Discards tokens that do not add information for parsing.
  struct IsJunkToken {
    bool operator()(const string& arString) const {
      return (arString == "APT" || arString == "UNIT");
    }
  };

  // Check if the address is parsed badly and should be omitted from results.
  struct IsAddressBadlyParsed {
    bool operator()(const Address& address) const {
      return (address.find("addr_num") == address.end() ||
	      address.find("street_name") == address.end());
    }
  };
}  // namespace

void AddressLookup::SetInAllAddresses(const string& key, const string& val,
				      vector<Address>* addresses) const {
  for (Address& address : *addresses) {
    address[key] = val;
  }
}

void AddressLookup::Parse(const string& address_str,
			  vector<Address>* addresses) const {
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
  address_tokens.erase(std::remove_if(address_tokens.begin(),
				      address_tokens.end(),
				      IsJunkToken()),
		       address_tokens.end());
  // The token index of the street name varies based on how the street number
  // is specified (ex: 14D vs 14 D).
  size_t street_name_idx = 0;

  // The token index of the (possible) apartment number varies on whether the
  // street had a street type.
  size_t unit_number_idx = -1;

  // Find the address number, especting it the first token in the string.
  boost::match_results<std::string::const_iterator> address_num_result;
  boost::regex address_num_regex("([0-9\\-]+)([A-Z]?)");

  // Index of the current token index for (multi-)address number parsing.
  size_t cur_addr_num_idx = 0;

  unordered_map<string, string> numbers = {
    {"ONE", "1"},
    {"TWO" , "2"},
    {"THREE", "3"},
    {"FOUR", "4"},
    {"FIVE", "5"}
  };

  // Some addresses spell out the number.
  while (numbers.find(address_tokens[cur_addr_num_idx]) != numbers.end()) {
    Address address;
    address["addr_num"] = numbers[address_tokens[cur_addr_num_idx]];
    addresses->push_back(address);
    cur_addr_num_idx++;
    street_name_idx++;
  }

  while (boost::regex_match(address_tokens[cur_addr_num_idx],
			    address_num_result,
			    address_num_regex)) {

    Address address;
    string token(address_num_result[1].first, address_num_result[1].second);
    address["addr_num"] = token;
    string addr_n_suffix(address_num_result[2].first,
			 address_num_result[2].second);
    cur_addr_num_idx++;
    street_name_idx++;
    if (addr_n_suffix.length() != 0) {
      address["addr_n_suffix"] = addr_n_suffix;
    }
    else if (address_tokens[cur_addr_num_idx].length() == 1) {
      address["addr_n_suffix"] = address_tokens[cur_addr_num_idx];
      // If the post-incremented addr index is a suffix, we skip ahead
      // once more.
      cur_addr_num_idx++;
      street_name_idx++;
    }
    // We also want to interpret ranges of numbers for multi-unit buildings.
    vector<string> numbers;
    string_split(address["addr_num"], '-', &numbers);
    addresses->push_back(address);
    if (numbers.size() == 2) {
      (*addresses)[0]["addr_num"] = numbers.at(0);
      int start_num = atoi(numbers[0].c_str());
      int end_num = atoi(numbers[1].c_str());
      for (int i = start_num + 1; i <= end_num; i += 2) {
	Address next_address;
	next_address["addr_num"] = std::to_string(i);
	addresses->push_back(next_address);
      }
    }
  }

  // Street names do not have numbers in the second word of the street.
  // Pick up N-tuples of tokens after the address-number, until we
  // encounter something non-alphabetical.
  size_t street_name_end_idx = address_tokens.size();
  while (street_name_end_idx >= street_name_idx) {
    std::stringstream ss;
    for(size_t i = street_name_idx; i < street_name_end_idx; ++i) {
      if(i != street_name_idx)
	ss << " ";
      ss << address_tokens[i];
    }
    if (street_names_.find(ss.str()) != street_names_.end()) {
      SetInAllAddresses("street_name", street_names_.at(ss.str()),
			addresses);
      unit_number_idx = street_name_end_idx;
      break;
    }
    street_name_end_idx--;
  }

  // Try to find a street type (ex: Ave, St, etc) -- there may not be one!
  for (const string& token : address_tokens) {
    if (street_types_.find(token) != street_types_.end()) {
      SetInAllAddresses("street_type", street_types_.at(token),
			addresses);
      unit_number_idx++;
    }
  }
  // Finally, we want to check for an apartment number, which again may not exist.
  if (unit_number_idx > 0 && unit_number_idx < address_tokens.size()) {
    // Join the remaining tokens and try to match a known apartment number.
    std::stringstream ss;
    for(size_t i = unit_number_idx; i < address_tokens.size(); ++i) {
      if(i != unit_number_idx) {
	ss << " ";
      }
      ss << address_tokens[i];
    }
    if (unit_numbers_.find(ss.str()) != unit_numbers_.end()) {
      SetInAllAddresses("unit_num", ss.str(), addresses);
    }
  }
  addresses->erase(std::remove_if(addresses->begin(),
				  addresses->end(),
				  IsAddressBadlyParsed()),
		   addresses->end());
}

int AddressLookup::Initialize() {
  connection_.reset(new mysqlpp::Connection(false));
  connection_->connect("sfabuse_dev", "localhost", "root", "");
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
    unit_numbers_[unit_num] = unit_num;
  }
  return 0;
}

void AddressLookup::LookupByAddresses(vector<Address>& to_lookup,
				      vector<Address>* found_addresses,
				      size_t limit) {
  if (to_lookup.empty()) {
    return;
  }
  vector<string> where_clauses_list;
  Query query = connection_->query();
  for (const Address& address : to_lookup) {
    string where_clause = "";
    bool first = true;
    for (const auto& iter : address) {
      if (iter.first == "unit_num" || iter.first == "addr_n_suffix") {
	continue;  // Unit numbers and number suffixes aren't well standardized.
	// Try later.
      }
      string val = iter.second;
      query.escape_string(&val);
      if (!first) {
	where_clause += " AND ";
      } else {
	first = false;
      }
      where_clause += ("("+iter.first + "=\"" + val + "\")");
    }
    where_clauses_list.push_back(where_clause);
  }

  // Where fields.
  std::stringstream ss_where;
  ss_where << "(";
  for(size_t i = 0; i < where_clauses_list.size(); ++i) {
    if(i != 0) {
      ss_where << ") OR (";
    }
    ss_where << where_clauses_list[i];
  }
  ss_where << ")";

  // Select fields.
  std::stringstream ss_select;
  const size_t field_num = 9;
  const char* field_list[] = {"addr_n_suffix", "addr_num", "block_lot",
			      "longitude", "latitude", "street_name",
			      "street_type", "unit_num", "zipcode"};
  for(size_t i = 0; i < field_num; ++i) {
    if(i != 0) {
      ss_select << ", ";
    }
    ss_select << field_list[i];
  }

  query << "SELECT " + ss_select.str() + " FROM address WHERE " + \
    ss_where.str();
  StoreQueryResult result = query.store();
  if (query.errnum() != 0) {
    std::cout << "Last Error: " << query.error() << std::endl;
    return;
  }
  size_t result_count = result.num_rows();
  found_addresses->resize(result_count);
  for (size_t i = 0; i < result_count; i++) {
    size_t column_idx = 0;
    for (const auto& iter : result[i]) {
      (*found_addresses)[i][field_list[column_idx]] = string(iter);
      column_idx++;
    }
  }
}
