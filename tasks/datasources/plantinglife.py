import json
import os
import re
from dataclasses import dataclass

import dotenv
import luigi
import mysql.connector
from mysql.connector.abstracts import MySQLConnectionAbstract
from mysql.connector.pooling import PooledMySQLConnection

from tasks.datasources import chatgpt
from tasks.datasources.usda import usda
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


class ExtractPlants(luigi.Task):
    """Extracts the plants table into a json file which maps lowercase scientific name to plant."""

    def output(self):
        return [
            luigi.LocalTarget("data/raw/plantinglife/plants.json"),
        ]

    def run(self):
        dotenv.load_dotenv()

        conn = get_db_connection()
        cursor = conn.cursor()
        cursor.execute(
            "SELECT id, scientific_name, common_name, bloom, pollinator_rating, bird_rating, "
            + "usda_source, wiki_source, wildflower_source, "
            + "height, spread, spread_rating, deer_resistance_rating, moistures, shades, habits "
            + "FROM plants"
        )

        name_to_plant = {}

        for (
            id,
            scientific_name,
            common_name,
            bloom,
            pollinator_rating,
            bird_rating,
            usda_source,
            wiki_source,
            wildflower_source,
            height,
            spread,
            spread_rating,
            deer_resistance,
            moistures,
            shades,
            habits,
        ) in cursor:  # type: ignore
            name_to_plant[scientific_name.lower()] = {
                "id": id,
                "common_name": common_name,
                "bloom": bloom,
                "pollinator_rating": pollinator_rating,
                "bird_rating": bird_rating,
                "usda_source": usda_source,
                "wiki_source": wiki_source,
                "wildflower_source": wildflower_source,
                "height": height,
                "spread": spread,
                "spread_rating": spread_rating,
                "deer_resistance_rating": deer_resistance,
                "moistures": list(moistures),
                "shades": list(shades),
                "habits": list(habits) if habits else [],
            }

        with self.output()[0].open("w") as f:
            f.write(json.dumps(name_to_plant, indent=2))


@dataclass
class Measurement:
    value: int
    unit: str

    def __init__(self, s: str):
        match = re.fullmatch(r"(\d+)\s*(\D+)", s)
        if not match:
            raise ValueError(f"Invalid Measurement format: {s!r}")

        self.value = int(match.group(1))
        self.unit = match.group(2)


@dataclass
class MeasurementRange:
    min: Measurement
    max: Measurement

    def __init__(self, s: str):
        split = s.split("-")
        self.min = Measurement(split[0].strip())
        self.max = Measurement(split[1].strip())


class TransformHabit(luigi.Task):
    """Determines a plant's habits, by making a few adjustments to USDA's classification. USDA's
    classifications don't match a layman's view of tree vs shrub, so this reclassifies a few
    based on height.

    Input: scientific name of plant (genus + species)
    Output: A JSON object with:
        "habits": the habit of this plant (tree, shrub, grass, etc)
    """

    task_namespace = "plantinglife"

    scientific_name: str = luigi.Parameter()  # type: ignore

    def requires(self):
        return [
            usda.TransformHabit(scientific_name=self.scientific_name),
            chatgpt.TransformHeight(scientific_name=self.scientific_name),
        ]

    def output(self):
        return [
            luigi.LocalTarget(
                f"data/transformed/plantinglife/habits/{self.scientific_name}.json"
            )
        ]

    def run(self):
        with (
            self.input()[0].open() as usda_habits_json,
            self.input()[1].open() as chatgpt_height_json,
        ):
            usda_habits_object = json.load(usda_habits_json)
            pl_habits: list[str] = usda_habits_object["habits"]

            habit_source = usda_habits_object["habit_source"]
            habit_source_detail = usda_habits_object["habit_source_detail"]

            if "tree" in pl_habits and "shrub" in pl_habits:
                chatgpt_height_object = json.load(chatgpt_height_json)
                height = chatgpt_height_object["height"]

                height_range = MeasurementRange(height)

                # to roughly match a layman's view of tree vs shrub...
                # a tree must be fairly tall and a shrub can't be too tall
                if height_range.min.value <= 8 and height_range.min.unit == "'":
                    pl_habits.remove("tree")
                    habit_source = "USDA, adjusted"
                    habit_source_detail += ", removed tree"
                elif height_range.max.value >= 35 and height_range.max.unit == "'":
                    pl_habits.remove("shrub")
                    habit_source = "USDA, adjusted"
                    habit_source_detail += ", removed shrub"

            result = {
                "habits": pl_habits,
                "habit_source": habit_source,
                "habit_source_detail": habit_source_detail,
            }

            with self.output()[0].open("w") as f:
                f.write(json.dumps(result))
