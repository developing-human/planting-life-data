import csv
import json
import os

import luigi

from tasks.datasources.plantinglife import ExtractPlantIds
from tasks.datasources.usda.location import TransformPlantZipcodes


class GeneratePlantsZipcodesCsv(luigi.Task):
    plants_filename: str = luigi.Parameter()  # type: ignore

    def output(self):  # type: ignore
        filename = os.path.basename(self.plants_filename)
        filename_no_ext = os.path.splitext(filename)[0]
        return [luigi.LocalTarget(f"data/out/plants-zipcodes-{filename_no_ext}.csv")]

    def run(self):
        with open(self.plants_filename) as plant_file:
            scientific_names = plant_file.read().splitlines()

        fields = [
            "scientific_name",
            "zipcode",
        ]
        with self.output()[0].open("w") as out:
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


# TODO: This approach was too slow to import, the CSV infile was much faster.
#       I plan to reuse parts of this to setup a diff, that will be used for future migrations.

# class GeneratePlantsZipcodesSql(luigi.Task):
#     plants_filename: str = luigi.Parameter()  # type: ignore
#
#     def output(self):
#         filename = os.path.basename(self.plants_filename)
#         filename_no_ext = os.path.splitext(filename)[0]
#         return [luigi.LocalTarget(f"data/out/zipcodes-plants-{filename_no_ext}.sql")]
#
#     def requires(self):
#         return [
#             GeneratePlantsZipcodesCsv(plants_filename=self.plants_filename),
#             ExtractPlantIds(),
#         ]
#
#     def run(self):
#         with (
#             self.input()[0][0].open() as plant_zips_csv,
#             self.input()[1][0].open() as plant_ids,
#             self.output()[0].open("w") as out,
#         ):
#             plant_zip_reader = csv.DictReader(plant_zips_csv)
#             name_to_id = json.load(plant_ids)
#
#             # aggregate name/zip pairs into name -> zips
#             name_to_zips: dict[str, list[str]] = {}
#             for row in plant_zip_reader:
#                 name: str = row["scientific_name"]
#                 zip: str = row["zipcode"]
#
#                 if name in name_to_zips:
#                     name_to_zips[name].append(zip)
#                 else:
#                     name_to_zips[name] = [zip]
#
#             # TODO: Before building the query, perhaps build all the (id, zip)
#             #       then can just join them together for the query? And maybe
#             #       add a new line every 4-5 entries?
#
#             for name, zips in name_to_zips.items():
#                 # if an id can't be found, the plant needs to be added to the db first!
#                 id = name_to_id[name.lower()]
#
#                 statement = (
#                     f"-- zipcodes for {name}\n"
#                     + "INSERT INTO zipcodes_plants (zipcode, plant_id) VALUES\n"
#                 )
#
#                 # build strings like (90210, 123)
#                 zip_id_pair_strs = [f"({zip}, {id})" for zip in zips]
#
#                 # now, join them, inserting a newline every 5 sets.
#                 for idx, pair in enumerate(zip_id_pair_strs):
#                     statement += pair + ", "
#
#                     if idx % 5 == 4:
#                         statement += "\n"
#
#                 # remove trailing newline/comma
#                 statement = statement.rstrip("\n ,")
#                 # for idx, zip in zips.enumerate():
#                 #     statement += f"({id}, {zip})"
#
#                 out.write(statement + ";\n\n")


# Generates an "infile" to import initial data into the join table.
# Data is loaded like:
# LOAD DATA LOCAL INFILE 'data/out/zipcodes-plants-all-infile.csv'
# INTO TABLE zipcodes_plants
# FIELDS TERMINATED BY ','
# LINES TERMINATED BY '\n'
# (zipcode, plant_id);
#
# For this to run efficiently with ~15M rows), drop & recreate the primary key before running.
class GeneratePlantsZipcodesCsvInfile(luigi.Task):
    plants_filename: str = luigi.Parameter()  # type: ignore

    def output(self):
        filename = os.path.basename(self.plants_filename)
        filename_no_ext = os.path.splitext(filename)[0]
        return [
            luigi.LocalTarget(f"data/out/zipcodes-plants-{filename_no_ext}-infile.csv")
        ]

    def requires(self):
        return [
            GeneratePlantsZipcodesCsv(plants_filename=self.plants_filename),
            ExtractPlantIds(),
        ]

    def run(self):
        with (
            self.input()[0][0].open() as plant_zips_csv,
            self.input()[1][0].open() as plant_ids,
            self.output()[0].open("w") as out,
        ):
            plant_zip_reader = csv.DictReader(plant_zips_csv)
            name_to_id = json.load(plant_ids)

            # aggregate name/zip pairs into name -> zips
            for row in plant_zip_reader:
                name: str = row["scientific_name"]
                zip: str = row["zipcode"]
                plant_id = name_to_id[name.lower()]
                out.write(f"{zip},{plant_id}\n")
