import csv
import luigi
from datetime import datetime
import tasks.datasources.usda as usda
import tasks.datasources.wildflower as wildflower


class GeneratePlantsCsv(luigi.Task):
    plants_filename: str = luigi.Parameter()

    def output(self):
        now = datetime.utcnow().strftime("%Y%m%d-%H%M%S")
        return luigi.LocalTarget(f"cache/tables/plants-{now}.csv")

    def run(self):
        with open(self.plants_filename) as plant_file:
            scientific_names = plant_file.read().splitlines()

        fields = ["scientific_name", "common_name", "moisture", "shade"]
        with self.output().open("w") as out:
            csv_out = csv.DictWriter(out, fields)
            csv_out.writeheader()

            for i, scientific_name in enumerate(scientific_names):
                print(
                    f"Processing {scientific_name} ({i+1} of {len(scientific_names)})"
                )
                common_name_task = usda.TransformCommonName(scientific_name)
                moisture_task = wildflower.TransformMoisture(scientific_name)
                shade_task = wildflower.TransformShade(scientific_name)
                luigi.build(
                    [common_name_task, moisture_task, shade_task],
                    workers=3,
                    local_scheduler=True,
                )

                row_out = {"scientific_name": scientific_name}
                with common_name_task.output().open() as f:
                    row_out["common_name"] = f.read().strip()
                with moisture_task.output().open() as f:
                    row_out["moisture"] = f.read().strip()
                with shade_task.output().open() as f:
                    row_out["shade"] = f.read().strip()

                csv_out.writerow(row_out)
