import csv
import json
import luigi
import os
import tasks.datasources.flickr as flickr


class GenerateImagesWithoutHumanOverridesCsv(luigi.Task):
    plants_filename: str = luigi.Parameter()

    def output(self):
        filename = os.path.basename(self.plants_filename)
        filename_no_ext = os.path.splitext(filename)[0]
        return luigi.LocalTarget(f"data/out/images-without-human-overrides-{filename_no_ext}.csv")

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

class GenerateImagesCsv(luigi.Task):
    plants_filename: str = luigi.Parameter()

    def output(self):
        filename = os.path.basename(self.plants_filename)
        filename_no_ext = os.path.splitext(filename)[0]
        return luigi.LocalTarget(f"data/out/images-{filename_no_ext}.csv")

    def requires(self):
        return GenerateImagesWithoutHumanOverridesCsv(plants_filename=self.plants_filename)

    def run(self):
        # originals are data that was automatically gathered
        with self.input().open() as csvfile:
            reader = csv.DictReader(csvfile)
            originals = list(reader)
            
        # human overrides are data that someone defined to override
        # the automatically generated data.  convert to dict for fast lookups.
        human_overrides_filename = "data/in/human-choices/images.csv"
        with open(human_overrides_filename, "r", newline="") as csvfile:
            reader = csv.DictReader(csvfile)
            overrides = { item["scientific_name"]: item for item in reader }
            
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
            
            for original in originals:
                scientific_name = original["scientific_name"]
                override = overrides.get(scientific_name, None)
                
                if override:
                    csv_out.writerow(override)
                else:
                    csv_out.writerow(original)

class GenerateImagesSql(luigi.Task):
    plants_filename: str = luigi.Parameter()

    def output(self):
        filename = os.path.basename(self.plants_filename)
        filename_no_ext = os.path.splitext(filename)[0]
        return luigi.LocalTarget(f"data/out/images-{filename_no_ext}.sql")

    def requires(self):
        return GenerateImagesCsv(plants_filename=self.plants_filename)

    def run(self):
        with self.input().open() as plant_csv, self.output().open("w") as out:
            reader = csv.DictReader(plant_csv)

            for row in reader:
                scientific_name = row["scientific_name"]
                title = row["title"].replace("'", "''")

                select_image_id_sql = (
                    "SELECT image_id "
                    + "FROM plants "
                    + f"WHERE scientific_name = '{scientific_name}'"
                )

                # TODO: This SQL will update existing images, but not insert new ones
                #       I think once I flip the relationship so images point to plants
                #       I'll be able to issue deletes/inserts easier.
                sql = (
                    "UPDATE images \n"
                    + f"SET title = '{title}',\n"
                    + f"    author = '{row['author']}',\n"
                    + f"    license = '{row['license']}',\n"
                    + f"    original_url = '{row['original_url']}',\n"
                    + f"    card_url = '{row['card_url']}'\n"
                    + f"WHERE id = ({select_image_id_sql});"
                )
                out.write(sql + "\n")
