import csv
import json
import luigi
import os
import tasks.datasources.usda as usda
import tasks.datasources.wildflower as wildflower
import tasks.datasources.chatgpt as chatgpt


class GeneratePlantsCsv(luigi.Task):
    plants_filename: str = luigi.Parameter()

    def output(self):
        filename = os.path.basename(self.plants_filename)
        filename_no_ext = os.path.splitext(filename)[0]
        return luigi.LocalTarget(f"data/out/plants-{filename_no_ext}.csv")

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
            "pollinator_rating",
            "bird_rating",
            "spread_rating",
            "deer_resistance_rating",
            "moisture_source",
            "moisture_source_detail",
            "shade_source",
            "shade_source_detail",
        ]
        with self.output().open("w") as out:
            csv_out = csv.DictWriter(out, fields)
            csv_out.writeheader()

            for i, scientific_name in enumerate(scientific_names):
                print(
                    f"Processing {scientific_name} ({i+1} of {len(scientific_names)})"
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
                ]

                luigi.build(
                    tasks,
                    workers=len(tasks),
                    local_scheduler=True,
                )

                row_out = {"scientific_name": scientific_name.capitalize()}
                for task in tasks:
                    with task.output().open() as f:
                        json_str = f.read().strip()
                        if json_str:
                            parsed = json.loads(json_str)

                            # Switch True/False to yes/no for csv
                            for k, v in parsed.items():
                                if v is True:
                                    parsed[k] = "yes"
                                elif v is False:
                                    parsed[k] = "no"

                            row_out.update(parsed)

                csv_out.writerow(row_out)


class GeneratePlantsSql(luigi.Task):
    plants_filename: str = luigi.Parameter()

    def output(self):
        filename = os.path.basename(self.plants_filename)
        filename_no_ext = os.path.splitext(filename)[0]
        return luigi.LocalTarget(f"data/out/plants-{filename_no_ext}.sql")

    def requires(self):
        return GeneratePlantsCsv(plants_filename=self.plants_filename)

    def run(self):
        with self.input().open() as plant_csv, self.output().open("w") as out:
            reader = csv.DictReader(plant_csv)

            def to_conditions_str(
                row: dict, none_field: str, some_field: str, lots_field: str
            ) -> str:
                conditions = []
                if row[none_field] == "yes":
                    conditions.append("None")
                if row[some_field] == "yes":
                    conditions.append("Some")
                if row[lots_field] == "yes":
                    conditions.append("Lots")

                return ",".join(conditions)

            for row in reader:
                shades_str = to_conditions_str(
                    row, "full_sun", "part_shade", "full_shade"
                )
                moistures_str = to_conditions_str(
                    row, "low_moisture", "medium_moisture", "high_moisture"
                )
                scientific_name = row["scientific_name"]

                # height & width may have ' in them, so escape them
                height = row["height"].replace("'", "''")
                width = row["width"].replace("'", "''")

                sql = (
                    "UPDATE plants \n"
                    + f"SET shades = '{shades_str}',\n"
                    + f"    moistures = '{moistures_str}',\n"
                    + f"    height = '{height}',\n"
                    + f"    spread = '{width}',\n"
                    + f"    bloom = '{row['bloom']}',\n"
                    # since its subtle... I removed the quotes on these numeric fields
                    + f"    pollinator_rating = {row['pollinator_rating']},\n"
                    + f"    bird_rating = {row['bird_rating']},\n"
                    + f"    spread_rating = {row['spread_rating']},\n"
                    + f"    deer_resistance_rating = {row['deer_resistance_rating']}\n"  # no comma at end
                    + f"WHERE scientific_name = '{scientific_name}';"
                )
                out.write(sql + "\n")


class AggregateFieldTask(luigi.Task):
    def run(self):
        for task in self.get_prioritized_tasks():
            yield task

            output = task.output().open("r").read()
            if output.strip():
                with self.output().open("w") as f:
                    f.write(output)
                    break

    def get_prioritized_tasks(self) -> list[luigi.Task]:
        raise NotImplementedError(
            "Must implement AggregateFieldTask.get_prioritized_tasks"
        )


class AggregateShade(AggregateFieldTask):
    scientific_name: str = luigi.Parameter()

    def output(self):
        return luigi.LocalTarget(f"data/aggregated/shade/{self.scientific_name}.json")

    def get_prioritized_tasks(self) -> list[luigi.Task]:
        return [
            wildflower.TransformShade(scientific_name=self.scientific_name),
            chatgpt.TransformShade(scientific_name=self.scientific_name),
        ]


class AggregateMoisture(AggregateFieldTask):
    scientific_name: str = luigi.Parameter()

    def output(self):
        return luigi.LocalTarget(
            f"data/aggregated/moisture/{self.scientific_name}.json"
        )

    def get_prioritized_tasks(self) -> list[luigi.Task]:
        return [
            wildflower.TransformMoisture(scientific_name=self.scientific_name),
            chatgpt.TransformMoisture(scientific_name=self.scientific_name),
        ]
