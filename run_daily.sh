#/bin/bash
export DATABASE_TARGET=PROD
export STARTDATE=$(date --date="last week" +%Y%m%d)
export ENDDATE=$(date +%Y%m%d)

mkdir -p /tmp/civic_import
python ./ingestion/record/recordscraper/recordScraper.py "${STARTDATE}:${ENDDATE}" NOTICE_OF_CONSTRAINTS_ON_REAL_PROPERTY /tmp/civic_import
python ./rebuild.py -t RECORD -d ${DATABASE_TARGET} -f "/tmp/civic_import/*.json"
casperjs ingestion/scrapers/sfrealtors.js
rm -rf /tmp/civic_import
