import os

import luigi

from tasks.tables.images.generate import GenerateImagesSql
from tasks.tables.plant_zipcodes.generate import GeneratePlantsZipcodesSqlDiff
from tasks.tables.plants.generate import GeneratePlantsSql


class GenerateAllSql(luigi.Task):
    plants_filename: str = luigi.Parameter()  # type: ignore

    def requires(self):
        return [
            GeneratePlantsSql(plants_filename=self.plants_filename),
            GeneratePlantsZipcodesSqlDiff(plants_filename=self.plants_filename),
            GenerateImagesSql(plants_filename=self.plants_filename),
        ]

    def output(self):
        filename = os.path.basename(self.plants_filename)
        filename_no_ext = os.path.splitext(filename)[0]
        return [luigi.LocalTarget(f"data/out/combined-sql-{filename_no_ext}.sql")]

    def run(self):
        with (
            self.input()[0][0].open() as plants_sql,
            self.input()[1][0].open() as locations_sql,
            self.input()[2][0].open() as images_sql,
            self.output()[0].open("w") as out,
        ):
            out.write(plants_sql.read())
            out.write(locations_sql.read())
            out.write(images_sql.read())
