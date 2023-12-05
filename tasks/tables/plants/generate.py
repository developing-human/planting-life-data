import csv
import json
import luigi
import tasks.datasources.usda as usda
import tasks.datasources.wildflower as wildflower


class GeneratePlantsCsv(luigi.Task):
    plants_filename: str = luigi.Parameter()

    def output(self):
        return luigi.LocalTarget("data/out/plants.csv")

    def complete(self):
        # Always run this Task, even if the output file exists
        return False

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
                    wildflower.TransformMoisture(scientific_name),
                    wildflower.TransformShade(scientific_name),
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
