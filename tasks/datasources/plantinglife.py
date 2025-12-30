import json
import os

import dotenv
import luigi
import mysql.connector

from tasks.lenient import LenientTask


class ExtractPlantIds(LenientTask):
    """Plant ids from the planting life database.

    Input: None
    Output: JSON object, mapping lowercase scientific name to id
    """

    def output(self):
        return [
            luigi.LocalTarget("data/raw/plantinglife/plant_ids.json"),
        ]

    def run_lenient(self):
        dotenv.load_dotenv()
        print("extract from wildflower (api call)")

        conn = mysql.connector.connect(
            host="127.0.0.1",
            user=os.getenv("DB_USER"),
            password=os.getenv("DB_PASSWORD"),
            database="planting_life",
        )

        cursor = conn.cursor()
        cursor.execute("select id, scientific_name from plants")

        scientific_name_to_id = {name.lower(): id for (id, name) in cursor}  # type: ignore

        with self.output()[0].open("w") as f:
            f.write(json.dumps(scientific_name_to_id, indent=2))
