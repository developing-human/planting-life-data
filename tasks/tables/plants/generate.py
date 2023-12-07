import csv
import json
import luigi
import tasks.datasources.usda as usda
import tasks.datasources.wildflower as wildflower
import tasks.datasources.chatgpt as chatgpt


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
                    AggregateMoisture(scientific_name),
                    AggregateShade(scientific_name),
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
