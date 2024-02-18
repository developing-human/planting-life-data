#!/bin/bash -e

# Check if exactly one argument is provided
if [ "$#" -ne  1 ]; then
    echo "Usage: $0 table_name"
    exit  1
fi

# Store the argument in the variable $table
table="$1"

# Read the db password
db_root_pw=`pass local-planting-life-db-root`

# create csv in /var/lib/mysql/planting_life/table.csv
echo "SELECT * INTO OUTFILE 'table.csv' FIELDS TERMINATED BY ',' OPTIONALLY ENCLOSED BY '\"' LINES TERMINATED BY '\n' FROM $table" | mysql -h 127.0.0.1 -P 3306 -u root planting_life --password=$db_root_pw

# write the csv to a temp file, replace \" with "" to more properly escape
tmp_file=`mktemp`
docker exec planting_life_db cat /var/lib/mysql/planting_life/table.csv | sed 's/\\"/\"\"/g' > $tmp_file

# remove table.csv from the docker container, this makes it cleaner
# to drop the database (ex: when loading prod into local)
docker exec planting_life_db rm /var/lib/mysql/planting_life/table.csv

cat $tmp_file