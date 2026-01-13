import csv
import json
import os
from typing import Iterable

import luigi

from tasks.datasources.plantinglife import (
    ExtractPlantIds,
    ExtractZipcodesPlants,
    TransformSpecificPlantIds,
)
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


# Compares the current local database with the newly generated plant locations,
# and generates a sql migration which makes the database match the new locations.
class GeneratePlantsZipcodesSqlDiff(luigi.Task):
    plants_filename: str = luigi.Parameter()  # type: ignore

    def output(self):
        filename = os.path.basename(self.plants_filename)
        filename_no_ext = os.path.splitext(filename)[0]
        return [
            luigi.LocalTarget(f"data/out/zipcodes-plants-diff-{filename_no_ext}.sql")
        ]

    def requires(self):
        return [
            GeneratePlantsZipcodesCsv(plants_filename=self.plants_filename),
            ExtractZipcodesPlants(),
            TransformSpecificPlantIds(plants_filename=self.plants_filename),
        ]

    @staticmethod
    def build_new_plant_to_zipcodes(
        plant_zips_csv: Iterable[str], name_to_id: dict[str, int]
    ) -> dict[int, set[int]]:
        plant_zip_reader = csv.DictReader(plant_zips_csv)

        # aggregate name/zip pairs into name -> zips
        id_to_zips: dict[int, set[int]] = {}
        for row in plant_zip_reader:
            name: str = row["scientific_name"]
            zip = int(row["zipcode"])

            id = name_to_id[name.lower()]

            if id in id_to_zips:
                id_to_zips[id].add(zip)
            else:
                id_to_zips[id] = set([zip])

        return id_to_zips

    @staticmethod
    def build_old_plant_to_zipcodes(
        plant_id_to_zips: dict[int, list[int]],
    ) -> dict[int, set[int]]:
        return {int(plant_id): set(zips) for plant_id, zips in plant_id_to_zips.items()}

    @staticmethod
    def add_plant_zips_sql(plant_name: str, plant_id: int, zips: set[int]):
        statement = (
            f"-- adding zipcodes for {plant_name}\n"
            + "INSERT INTO zipcodes_plants (zipcode, plant_id) VALUES\n"
        )

        # build strings like (90210, 123)
        zip_id_pair = [f"({zip}, {plant_id})" for zip in zips]

        # now, join them, inserting a newline every 5 sets.
        for idx, pair in enumerate(zip_id_pair):
            statement += pair + ", "

            if idx % 5 == 4:
                statement += "\n"

        # remove trailing newline/comma
        return statement.rstrip("\n ,") + ";\n\n"

    @staticmethod
    def delete_plant_zips_sql(plant_name: str, plant_id: int, zips: set[int]):
        statement = (
            f"-- removing zipcodes for {plant_name}\n"
            + f"DELETE FROM zipcodes_plants WHERE plant_id = {plant_id} AND zipcode IN (\n"
        )

        # now, join them, inserting a newline every 5 sets.
        for idx, zip in enumerate(zips):
            statement += str(zip) + ", "

            if idx % 5 == 4:
                statement += "\n"

        # remove trailing newline/comma
        statement = statement.rstrip("\n ,")

        # close parens/statement
        statement += ");\n\n"

        return statement

    def run(self):
        with (
            self.input()[0][0].open() as new_plant_zips_csv,
            self.input()[1][0].open() as old_plant_id_to_zips,
            self.input()[2][0].open() as all_plant_ids_json,
            self.output()[0].open("w") as out,
        ):
            all_plant_ids = json.loads(all_plant_ids_json.read())

            old_name_to_id = all_plant_ids["existing_name_to_id"]
            new_name_to_id = all_plant_ids["new_name_to_id"]
            all_names_to_id = dict(old_name_to_id)
            all_names_to_id |= new_name_to_id

            all_ids_to_name = {id: name for name, id in old_name_to_id.items()}
            all_ids_to_name |= {id: name for name, id in new_name_to_id.items()}

            new_plant_to_zipcodes = self.build_new_plant_to_zipcodes(
                new_plant_zips_csv, all_names_to_id
            )
            old_plant_to_zipcodes = self.build_old_plant_to_zipcodes(
                json.loads(old_plant_id_to_zips.read()),
            )

            # look for new plants/zipcodes by comparing new to old
            for plant_id, new_zips in new_plant_to_zipcodes.items():
                plant_name = all_ids_to_name[plant_id]
                old_zips = old_plant_to_zipcodes.get(plant_id, None)

                if old_zips is None:
                    # plant is new, so add all the zips
                    out.write(self.add_plant_zips_sql(plant_name, plant_id, new_zips))
                else:
                    # add zips that didn't exist before
                    zips_to_add = new_zips - old_zips
                    if zips_to_add:
                        out.write(
                            self.add_plant_zips_sql(plant_name, plant_id, zips_to_add)
                        )

                    # remove zips that no longer exist
                    zips_to_rm = old_zips - new_zips
                    if zips_to_rm:
                        out.write(
                            self.delete_plant_zips_sql(plant_name, plant_id, zips_to_rm)
                        )

            # when working with anything other than "all", it's wrong to assume that an unlisted plant should be deleted.
            if self.plants_filename.endswith("/all.txt"):
                # look for removed plants/zipcodes by comparing old to new
                #
                # TODO: When deleting, I think I want to run things backwards? How will I wire that up?
                for plant_id, _ in old_plant_to_zipcodes.items():
                    if plant_id not in new_plant_to_zipcodes:
                        out.write(
                            f"-- removing zips for {all_ids_to_name.get(plant_id, plant_id)}\n"
                        )
                        out.write(
                            f"DELETE FROM zipcodes_plants WHERE plant_id = {plant_id};\n\n"
                        )


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
