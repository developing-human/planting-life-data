# This script takes chatgpt values out of the local database and writes
# them to "transformed" directory.  This lets us reuse everything that
# already lives in the database without querying chatgpt again.

import logging
import csv
import json
import os
import subprocess
from getpass import getpass
from io import StringIO

logging.getLogger().setLevel(logging.WARN)

fields_to_extract = [
    "pollinator_rating",
    "bird_rating",
    "spread_rating",
    "deer_resistance_rating",
    "height",
    "spread",
    "bloom",
]

base_dir = "data/transformed/chatgpt"


def query_database(password):
    """Simple but kind of janky way to run query against the
    database, results in a string in tsv format"""

    fields_str = ", ".join(fields_to_extract)
    cmd = f"""mysql -h localhost -P 3306 -u planting_life_user --protocol=tcp -p{password} \
    planting_life -e "SELECT scientific_name, {fields_str} FROM plants;" """
    process = subprocess.Popen(
        cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, shell=True
    )
    stdout, stderr = process.communicate()
    return stdout.decode()


if __name__ == "__main__":
    password = getpass("planting_life_user password:")

    db_result = query_database(password)
    reader = csv.DictReader(StringIO(db_result), delimiter="\t")

    for row in reader:
        scientific_name = row["scientific_name"].lower()

        for field in fields_to_extract:
            field_value = row[field]
            if field_value == "NULL":
                continue  # don't save null values

            try:
                field_value = int(row[field])
            except ValueError:
                pass  # keep it as a string if it can't parse as an int

            # width is called spread in the db, this is a janky fix to
            # account for the difference
            if field == "spread":
                field = "width"

            file_path = f"{base_dir}/{field}/{scientific_name}.json"
            if os.path.exists(file_path):
                continue  # file already exists, don't overwrite

            with open(file_path, "w") as f:
                print(f"Writing to {file_path}")
                formatted_output = json.dumps({field: field_value}, indent=4)
                f.write(formatted_output)
