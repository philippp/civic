CC=g++
CFLAGS=-std=c++11 -Wall
CLIBS=-pthread -lpcap -lmysqlpp -lmysqlclient -lboost_regex -ljsoncpp
CINCLUDES=-I$(PWD) -I/usr/include/mysql
CFLAGS+= -ggdb

all: civic

cgi_addresslookup: digestion/locations/address_lookup.o digestion/locations/address_lookup.cpp
	$(CC) $(CFLAGS) $(CLIBS) $(CINCLUDES) digestion/locations/cgi_addresslookup.cpp digestion/locations/address_lookup.o -o digestion/locations/cgi_addresslookup
	sudo cp -f ${PWD}/digestion/locations/cgi_addresslookup /usr/lib/cgi-bin/address_lookup
	sudo chmod a+x /usr/lib/cgi-bin/address_lookup

address_lookup_main: digestion/locations/address_lookup.o digestion/locations/address_lookup.cpp
	$(CC) $(CFLAGS) $(CLIBS) $(CINCLUDES) digestion/locations/address_lookup_main.cpp digestion/locations/address_lookup.o -o digestion/locations/address_lookup_main

digestion/locations/address_lookup.o: digestion/locations/address_lookup.cpp
	$(CC) -c $(CFLAGS) $(CLIBS) $(CINCLUDES) digestion/locations/address_lookup.cpp -o digestion/locations/address_lookup.o

clean:
	rm -rf `find . -name "*.o"` digestion/locations/address_main 