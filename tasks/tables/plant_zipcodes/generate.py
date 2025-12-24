import csv
import json
import os

import luigi

from tasks.datasources.usda.location import TransformPlantZipcodes


class GeneratePlantZipcodesCsv(luigi.Task):
    plants_filename: str = luigi.Parameter()  # type: ignore

    def output(self):  # type: ignore
        filename = os.path.basename(self.plants_filename)
        filename_no_ext = os.path.splitext(filename)[0]
        return luigi.LocalTarget(f"data/out/plants-zipcodes-{filename_no_ext}.csv")

    def run(self):
        with open(self.plants_filename) as plant_file:
            scientific_names = plant_file.read().splitlines()

        fields = [
            "scientific_name",
            "zipcode",
        ]
        with self.output().open("w") as out:
            csv_out = csv.DictWriter(out, fields)
            csv_out.writeheader()

            for i, scientific_name in enumerate(scientific_names):
                print(
                    f"Processing {scientific_name} ({i + 1} of {len(scientific_names)})"
                )

                tasks = [TransformPlantZipcodes(scientific_name)]

                luigi.build(
                    tasks,
                    workers=len(tasks),
                    local_scheduler=True,
                )

                row_out = {"scientific_name": scientific_name.capitalize()}
                for task in tasks:
                    with task.output()[0].open() as f:
                        json_str = f.read().strip()
                        if json_str:
                            parsed = json.loads(json_str)
                            for zipcode in parsed["native_zips"]:
                                row_out["zipcode"] = zipcode
                                csv_out.writerow(row_out)


# class GeneratePlantsSql(luigi.Task):
#     plants_filename: str = luigi.Parameter()  # type: ignore
#
#     def output(self):
#         filename = os.path.basename(self.plants_filename)
#         filename_no_ext = os.path.splitext(filename)[0]
#         return luigi.LocalTarget(f"data/out/plants-{filename_no_ext}.sql")
#
#     def requires(self):
#         return GeneratePlantsCsv(plants_filename=self.plants_filename)
#
#     def run(self):
#         with self.input().open() as plant_csv, self.output().open("w") as out:
#             reader = csv.DictReader(plant_csv)
#
#             def to_conditions_str(
#                 row: dict, none_field: str, some_field: str, lots_field: str
#             ) -> str:
#                 conditions = []
#                 if row[none_field] == "yes":
#                     conditions.append("None")
#                 if row[some_field] == "yes":
#                     conditions.append("Some")
#                 if row[lots_field] == "yes":
#                     conditions.append("Lots")
#
#                 return ",".join(conditions)
#
#             for row in reader:
#                 shades_str = to_conditions_str(
#                     row, "full_sun", "part_shade", "full_shade"
#                 )
#                 moistures_str = to_conditions_str(
#                     row, "low_moisture", "medium_moisture", "high_moisture"
#                 )
#                 scientific_name = row["scientific_name"]
#
#                 # height & width may have ' in them, so escape them
#                 height = row["height"].replace("'", "''")
#                 width = row["width"].replace("'", "''")
#
#                 sql = (
#                     "UPDATE plants \n"
#                     + f"SET shades = '{shades_str}',\n"
#                     + f"    moistures = '{moistures_str}',\n"
#                     + f"    height = '{height}',\n"
#                     + f"    spread = '{width}',\n"
#                     + f"    bloom = '{row['bloom']}',\n"
#                     # since its subtle... I removed the quotes on these numeric fields
#                     + f"    pollinator_rating = {row['pollinator_rating']},\n"
#                     + f"    bird_rating = {row['bird_rating']},\n"
#                     + f"    spread_rating = {row['spread_rating']},\n"
#                     + f"    deer_resistance_rating = {row['deer_resistance_rating']}\n"  # no comma at end
#                     + f"WHERE scientific_name = '{scientific_name}';"
#                 )
#                 out.write(sql + "\n")
