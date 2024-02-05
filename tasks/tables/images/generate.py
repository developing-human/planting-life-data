import csv
import json
import luigi
import os
import tasks.datasources.flickr as flickr


class GenerateImagesCsv(luigi.Task):
    plants_filename: str = luigi.Parameter()

    def output(self):
        filename = os.path.basename(self.plants_filename)
        filename_no_ext = os.path.splitext(filename)[0]
        return luigi.LocalTarget(f"data/out/images-{filename_no_ext}.csv")

    def run(self):
        with open(self.plants_filename) as plant_file:
            scientific_names = plant_file.read().splitlines()

        fields = [
            "scientific_name",
            "title",
            "author",
            "license",
            "original_url",
            "card_url",
        ]

        with self.output().open("w") as out:
            csv_out = csv.DictWriter(out, fields)
            csv_out.writeheader()

            for i, scientific_name in enumerate(scientific_names):
                print(
                    f"Processing {scientific_name} ({i+1} of {len(scientific_names)})"
                )

                tasks = [flickr.TransformBestFlickrImage(scientific_name)]

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

                            row_out.update(parsed)

                # the flickr image will have a few fields that shouldn't appear
                # in the csv, so filter the row to just the expected fields
                row_out = {key: row_out[key] for key in row_out if key in fields}

                if len(row_out) == len(fields):
                    csv_out.writerow(row_out)
                else:
                    print(f"Missing data, skipping row for {scientific_name}")
