# Notes for an eventual installer.
sudo apt-get install python-mysqldb python-dev python-pip libxml2-dev libxslt-dev python-xlrd libmysql++-dev libboost-all-dev libjsoncpp-dev

# OpenPyXL depends on this lxml version.
sudo pip install lxml==3.2.5 openpyxl

# Set up a database, load all of the schemas, then...

# Build C++ parts and copy CGI modules
make all

echo "cd $PWD; ./run_daily.sh; echo \"Ran run_daily.sh on \$(date +%Y%m%d)\" >> logs/import.log" >> civic_daily
chmod +x civic_daily
sudo mv civic_daily /etc/cron.daily