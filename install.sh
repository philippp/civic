# Notes for an eventual installer.
sudo apt-get install python-mysqldb
sudo apt-get install python-dev
sudo apt-get install python-pip
sudo apt-get install libxml2-dev libxslt-dev
sudo apt-get install python-xlrd
sudo apt-get install libmysql++-dev
sudo apt-get install libboost-all-dev
sudo apt-get install libjsoncpp-dev

# OpenPyXL depends on this lxml version.
sudo pip install lxml==3.2.5
sudo pip install openpyxl

# Set up a database, load all of the schemas, then...

# Build C++ parts and copy CGI modules
make civic