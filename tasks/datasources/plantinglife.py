import json
import os

import dotenv
import luigi
import mysql.connector
from mysql.connector.abstracts import MySQLConnectionAbstract
from mysql.connector.pooling import PooledMySQLConnection

from tasks.lenient import LenientTask


def get_db_connection() -> PooledMySQLConnection | MySQLConnectionAbstract:
    return mysql.connector.connect(
        host="127.0.0.1",
        user=os.getenv("DB_USER"),
        password=os.getenv("DB_PASSWORD"),
        database="planting_life",
    )


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

        conn = get_db_connection()
        cursor = conn.cursor()
        cursor.execute("select id, scientific_name from plants")

        scientific_name_to_id = {name.lower(): int(id) for (id, name) in cursor}  # type: ignore

        with self.output()[0].open("w") as f:
            f.write(json.dumps(scientific_name_to_id, indent=2))


class TransformSpecificPlantIds(LenientTask):
    """Given a list of scientific names, finds or creates ids for them.

    To differentiate existing vs new plants, outputs JSON like:
    {
        "existing_name_to_id": {
            "foo": 123
        },
        "new_name_to_id": {
            "bar": 321
        }
    }
    """

    plants_filename: str = luigi.Parameter()  # type: ignore

    def requires(self):
        return [ExtractPlantIds()]

    def output(self):
        filename = os.path.basename(self.plants_filename)
        filename_no_ext = os.path.splitext(filename)[0]
        return [
            luigi.LocalTarget(
                f"data/transformed/plantinglife/plant-ids-{filename_no_ext}.json"
            )
        ]

    def run_lenient(self):
        with open(self.plants_filename) as plant_file:
            scientific_names = plant_file.read().splitlines()

        with self.input()[0][0].open() as all_plant_ids_json:
            existing_name_to_id: dict[str, int] = json.loads(all_plant_ids_json.read())

            result = {
                "existing_name_to_id": {},
                "new_name_to_id": {},
            }

            next_generated_id = max(existing_name_to_id.values()) + 1
            for scientific_name in scientific_names:
                existing_id = existing_name_to_id.get(scientific_name, None)
                if existing_id is not None:
                    result["existing_name_to_id"][scientific_name] = existing_id
                else:
                    result["new_name_to_id"][scientific_name] = next_generated_id
                    next_generated_id += 1

        print(result)
        with self.output()[0].open("w") as f:
            f.write(json.dumps(result, indent=2))


class ExtractZipcodesPlants(LenientTask):
    """Extracts the zipcodes_plants join table into a json file."""

    def output(self):
        return [
            luigi.LocalTarget("data/raw/plantinglife/plants_zipcodes.json"),
        ]

    def run_lenient(self):
        dotenv.load_dotenv()

        conn = get_db_connection()
        cursor = conn.cursor()
        cursor.execute("select plant_id, zipcode from zipcodes_plants")

        plant_id_to_zipcodes = {}
        for plant_id, zipcode in cursor:  # type: ignore
            if plant_id not in plant_id_to_zipcodes:
                plant_id_to_zipcodes[plant_id] = [zipcode]
            else:
                plant_id_to_zipcodes[plant_id].append(zipcode)

        with self.output()[0].open("w") as f:
            f.write(json.dumps(plant_id_to_zipcodes))


class ExtractImages(LenientTask):
    """Extracts the images table into a json file which maps scientific name to image."""

    def output(self):
        return [
            luigi.LocalTarget("data/raw/plantinglife/images.json"),
        ]

    def run_lenient(self):
        dotenv.load_dotenv()

        conn = get_db_connection()
        cursor = conn.cursor()
        cursor.execute(
            "SELECT plants.scientific_name, plants.id as plant_id, images.id as id, title, card_url, original_url, author, license "
            + "FROM images "
            + "INNER JOIN plants ON plants.image_id = images.id"
        )

        name_to_image = {}

        for (
            scientific_name,
            plant_id,
            id,
            title,
            card_url,
            original_url,
            author,
            license,
        ) in cursor:  # type: ignore
            name_to_image[scientific_name] = {
                "plant_id": plant_id,
                "id": id,
                "title": title,
                "card_url": card_url,
                "original_url": original_url,
                "author": author,
                "license": license,
            }

        with self.output()[0].open("w") as f:
            f.write(json.dumps(name_to_image, indent=2))
