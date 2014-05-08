#include "digestion/locations/address_lookup.h"
#include <iostream>
#include <stdlib.h>
#include <jsoncpp/json/writer.h>
#include <boost/algorithm/string.hpp>
#include <unordered_map>

using std::string;
typedef unordered_map<string, vector<string>> ParamMap;

string UrlDecode(string &SRC) {
  string ret;
  char ch;
  for (int i = 0; i < SRC.length(); i++) {
    if (int(SRC[i])==37) {
      int ii;
      sscanf(SRC.substr(i+1,2).c_str(), "%x", &ii);
      ch = static_cast<char>(ii);
      ret += ch;
      i = i + 2;
    } else {
      ret += SRC[i];
    }
  }
  return (ret);
}

void GetGet(char* envp[], ParamMap* getargs) {
  for (int i=0; envp[i]; i++) {
    if (boost::starts_with(envp[i], "QUERY_STRING=")) {
      char* query_vals = &envp[i][13];
      vector<string> parts;
      boost::split(parts, query_vals, boost::is_any_of("&"));
      for (const string& part : parts) {
	vector<string> keyval;
	boost::split(keyval, part, boost::is_any_of("="));
	if (keyval.size() == 2) {
	  (*getargs)[keyval[0]].push_back(UrlDecode(keyval[1]));
	}
      }
    }
  }
}

int main(int argc, char* argv[], char* envp[]) {
  std::cout << "Content-type: text/html\n\n";
  // Figure out which address(es) to pull from the get param(s)
  ParamMap getargs;
  GetGet(envp, &getargs);
  AddressLookup al;
  al.Initialize();
  vector<Address> addresses;
  vector<Address> resolved_addresses;
  for (const string& address_str : getargs["address"]) {
    vector<Address> cur_addresses;
    al.Parse(address_str, &cur_addresses);
    addresses.insert(addresses.begin(), cur_addresses.begin(), cur_addresses.end());
  }
  al.LookupByAddresses(addresses, &resolved_addresses);

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
  std::cout << writer.write(root);
  return 0;
} 
