#!/bin/bash -e

# Check if exactly one argument is provided
if [ "$#" -ne  2 ]; then
    echo "Usage: $0 plants|images short|long|all"
    exit  1
fi

# Store the argument in the variable $table
table="$1"
migration="$2"

# If run from elsewhere, treat it like its running from diffs directory
cd ~/code/planting-life-data/diffs

# Remove the file if it exists
# Can't reload database if it exists
docker exec planting_life_db rm -f /var/lib/mysql/planting_life/table.csv

# Load prod into local
# This references a script in a different, private repo and I think that's ok
cd ../../planting-life-scripts/bash
./load_prod_db_into_local.sh

# Take the "before" snapshot of the table
cd - > /dev/null
./table_to_csv.sh $table > before.csv

# Apply migration
db_admin_pw=`pass local-planting-life-db-admin`
mysql -h 127.0.0.1 -P 3306 -u planting_life_admin planting_life --password=$db_admin_pw < ../data/out/$table-$migration.sql

# Take the "after" snapshot of the table
./table_to_csv.sh $table > after.csv


if [ "$table" == "plants" ]; then
  extra_params="-o word-diff --ignore-columns 7,8"
elif [ "$table" == "images" ]; then
  extra_params="-o json --columns 0,2"
else
  extra_params=""
fi

csvdiff before.csv after.csv $extra_params