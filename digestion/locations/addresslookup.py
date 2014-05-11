import ctypes
import json

lib = ctypes.cdll.LoadLibrary('digestion/locations/py_addresslookup.so')

class AddressLookup(object):
    def __init__(self):
        self.looker = lib.AddressLookup_new()
        
    def lookup(self, address_string):
        addr_str = lib.LookupAddress(self.looker, address_string)
        addresses_list = json.loads(
            ctypes.cast(addr_str, ctypes.c_char_p).value)
        lib.FreeAddressJSON(addr_str)
        return addresses_list

if __name__ == "__main__":
    a = AddressLookup()
    print a.lookup("3178 16TH ST")
