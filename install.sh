# Notes for an eventual installer.
sudo apt-get install apache2 mysql-server mysql-client python-mysqldb python-dev python-pip libxml2-dev libxslt-dev python-xlrd libmysql++-dev libboost-all-dev libjsoncpp-dev make build-essential python-software-properties

# OpenPyXL depends on this lxml version.
sudo pip install --upgrade lxml==3.3.1 openpyxl

# Set up a database, load all of the schemas, then...

# Build C++ parts and copy CGI modules
make all

echo "#!/bin/sh" >> civic_daily
echo "cd $PWD; ./run_daily.sh; echo \"Ran run_daily.sh on \$(date +%Y%m%d)\" >> logs/import.log" >> civic_daily
chmod +x civic_daily
sudo mv civic_daily /etc/cron.daily