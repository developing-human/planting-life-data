import csv
import json
import os
from typing import Any

import luigi

import tasks.datasources.chatgpt as chatgpt
import tasks.datasources.usda.usda as usda
import tasks.datasources.wildflower as wildflower
from tasks.datasources import wikipedia
from tasks.datasources.plantinglife import ExtractPlants, TransformSpecificPlantIds


class GeneratePlantsCsv(luigi.Task):
    plants_filename: str = luigi.Parameter()  # type: ignore

    def output(self):
        filename = os.path.basename(self.plants_filename)
        filename_no_ext = os.path.splitext(filename)[0]
        return [luigi.LocalTarget(f"data/out/plants-{filename_no_ext}.csv")]

    def run(self):
        with open(self.plants_filename) as plant_file:
            scientific_names = plant_file.read().splitlines()

        fields = [
            "scientific_name",
            "common_name",
            "full_shade",
            "part_shade",
            "full_sun",
            "low_moisture",
            "medium_moisture",
            "high_moisture",
            "bloom",
            "height",
            "width",
            "habits",
            "pollinator_rating",
            "bird_rating",
            "spread_rating",
            "deer_resistance_rating",
            "moisture_source",
            "moisture_source_detail",
            "shade_source",
            "shade_source_detail",
            "habit_source",
            "habit_source_detail",
            "usda_source",
            "wiki_source",
        ]
        with self.output()[0].open("w") as out:
            csv_out = csv.DictWriter(out, fields)
            csv_out.writeheader()

            for i, scientific_name in enumerate(scientific_names):
                print(
                    f"Processing {scientific_name} ({i + 1} of {len(scientific_names)})"
                )

                tasks = [
                    usda.TransformCommonName(scientific_name),
                    AggregateMoisture(scientific_name),
                    AggregateShade(scientific_name),
                    chatgpt.TransformBloom(scientific_name),
                    chatgpt.TransformHeight(scientific_name),
                    chatgpt.TransformWidth(scientific_name),
                    chatgpt.TransformPollinatorRating(scientific_name),
                    chatgpt.TransformBirdRating(scientific_name),
                    chatgpt.TransformSpreadRating(scientific_name),
                    chatgpt.TransformDeerResistanceRating(scientific_name),
                    usda.TransformHabit(scientific_name),
                    usda.TransformSourceUrl(scientific_name),
                    wikipedia.TransformSourceUrl(scientific_name),
                ]

                luigi.build(
                    tasks,
                    workers=len(tasks),
                    local_scheduler=True,
                )

                row_out = {"scientific_name": scientific_name.capitalize()}
                for task in tasks:
                    # some of the tasks have a single item as output, some have a list of one to be more type friendly
                    output = task.output()
                    if isinstance(output, list):
                        output = output[0]

                    with output.open() as f:
                        json_str = f.read().strip()
                        if json_str:
                            parsed = json.loads(json_str)

                            # Switch True/False to yes/no for csv
                            for k, v in parsed.items():
                                if v is True:
                                    parsed[k] = "yes"
                                elif v is False:
                                    parsed[k] = "no"
                                elif isinstance(v, list):
                                    parsed[k] = ",".join(v)

                            row_out.update(parsed)

                csv_out.writerow(row_out)


PLANT_DB_FIELDS = [
    "id",
    "scientific_name",
    "common_name",
    "bloom",
    "pollinator_rating",
    "bird_rating",
    "usda_source",
    "wiki_source",
    "height",
    "spread",
    "spread_rating",
    "deer_resistance_rating",
    "moistures",
    "shades",
    "habits",
]


class GeneratePlantsSql(luigi.Task):
    plants_filename: str = luigi.Parameter()  # type: ignore

    def output(self):
        filename = os.path.basename(self.plants_filename)
        filename_no_ext = os.path.splitext(filename)[0]
        return [luigi.LocalTarget(f"data/out/plants-{filename_no_ext}.sql")]

    def requires(self):
        return [
            GeneratePlantsCsv(plants_filename=self.plants_filename),
            TransformSpecificPlantIds(plants_filename=self.plants_filename),
            ExtractPlants(),
        ]

    @staticmethod
    def get_updated_fields(old: dict[str, Any], new: dict[str, Any]) -> dict[str, Any]:
        updated_fields = {}
        for field_name, new_value in new.items():
            if field_name not in PLANT_DB_FIELDS:
                continue

            old_value = old[field_name]

            if new_value == "":
                new_value = None

            if isinstance(old_value, list) and isinstance(new_value, list):
                old_value.sort()
                new_value.sort()

            if str(old_value) != str(new_value):
                updated_fields[field_name] = new_value

        return updated_fields

    @staticmethod
    def to_sql_setters(fields: dict[str, Any]) -> str:
        sql = "SET"
        spaces = "   "
        for field_name, value in fields.items():
            if isinstance(value, str):
                escaped = value.replace("'", "''")
                value_str = f"'{escaped}'"
            elif isinstance(value, int):
                value_str = f"{value}"
            elif isinstance(value, list):
                value_str = "'" + ",".join(value) + "'"
            elif value is None:
                value_str = "null"
            else:
                raise ValueError(f"unexpected type for {field_name}: {type(value)}")
            sql += f"{spaces}{field_name} = {value_str},\n"

            # just being particular about formatting...
            spaces = "      "

        return sql.rstrip(",\n ")

    @staticmethod
    def to_conditions(
        plant: dict, none_field: str, some_field: str, lots_field: str
    ) -> list[str]:
        conditions = []
        if plant.pop(none_field) == "yes":
            conditions.append("None")
        if plant.pop(some_field) == "yes":
            conditions.append("Some")
        if plant.pop(lots_field) == "yes":
            conditions.append("Lots")

        return conditions

    @staticmethod
    def to_habits(plant: dict) -> list[str]:
        habits = plant.get("habits", [])
        if not habits or habits == [""]:
            return []

        sanitized_habits = []
        for habit in habits.split(","):
            if habit == "flower-or-herb":
                habit = "FlowerOrHerb"
            else:
                habit = habit.capitalize()

            sanitized_habits.append(habit)

        return sanitized_habits

    def run(self):
        with (
            self.input()[0][0].open() as plant_csv,
            self.input()[1][0].open() as id_json,
            self.input()[2][0].open() as all_plants_json,
            self.output()[0].open("w") as out,
        ):  # type: ignore
            reader = csv.DictReader(plant_csv)
            ids = json.loads(id_json.read())
            all_names_to_plant = json.loads(all_plants_json.read())
            new_name_to_id = ids["new_name_to_id"]

            for row in reader:
                updated_plant = dict(row)
                updated_plant["shades"] = self.to_conditions(
                    updated_plant, "full_sun", "part_shade", "full_shade"
                )
                updated_plant["moistures"] = self.to_conditions(
                    updated_plant, "low_moisture", "medium_moisture", "high_moisture"
                )
                updated_plant["habits"] = self.to_habits(updated_plant)

                # TODO: These names aren't always consistent...
                updated_plant["spread"] = updated_plant.pop("width")

                updated_plant = {
                    key: value
                    for key, value in updated_plant.items()
                    if key in PLANT_DB_FIELDS
                }

                scientific_name = row["scientific_name"]
                existing_plant = all_names_to_plant.get(scientific_name.lower(), None)

                if existing_plant is None:
                    out.write(
                        (
                            "INSERT INTO plants \n"
                            + self.to_sql_setters(updated_plant)
                            + f",\n    id = {new_name_to_id[scientific_name.lower()]};\n\n"
                        )
                    )
                else:
                    existing_plant["scientific_name"] = scientific_name
                    updated_fields = self.get_updated_fields(
                        existing_plant, updated_plant
                    )
                    if updated_fields:
                        out.write(
                            (
                                "UPDATE plants \n"
                                + self.to_sql_setters(updated_fields)
                                + f"\nWHERE scientific_name = '{scientific_name}';\n\n"
                            )
                        )

            # TODO: Consider deleting a plant if we're processing all.txt and it isn't in the list.


class AggregateFieldTask(luigi.Task):
    def run(self):  # type: ignore
        for task in self.get_prioritized_tasks():
            yield task

            output = task.output().open("r").read()  # type: ignore
            if output.strip():
                with self.output().open("w") as f:  # type: ignore
                    f.write(output)
                    break

    def get_prioritized_tasks(self) -> list[luigi.Task]:
        raise NotImplementedError(
            "Must implement AggregateFieldTask.get_prioritized_tasks"
        )


class AggregateShade(AggregateFieldTask):
    scientific_name: str = luigi.Parameter()  # type: ignore

    def output(self):  # type: ignore
        return luigi.LocalTarget(f"data/aggregated/shade/{self.scientific_name}.json")

    def get_prioritized_tasks(self) -> list[luigi.Task]:
        return [
            wildflower.TransformShade(scientific_name=self.scientific_name),
            chatgpt.TransformShade(scientific_name=self.scientific_name),
        ]


class AggregateMoisture(AggregateFieldTask):
    scientific_name: str = luigi.Parameter()  # type: ignore

    def output(self):  # type: ignore
        return luigi.LocalTarget(
            f"data/aggregated/moisture/{self.scientific_name}.json"
        )

    def get_prioritized_tasks(self) -> list[luigi.Task]:
        return [
            wildflower.TransformMoisture(scientific_name=self.scientific_name),
            chatgpt.TransformMoisture(scientific_name=self.scientific_name),
        ]
