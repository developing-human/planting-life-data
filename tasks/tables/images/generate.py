import csv
import json
import os
from time import sleep
from typing import Any

import emoji
import luigi

import tasks.datasources.flickr as flickr
import tasks.datasources.inaturalist as inaturalist
from tasks.datasources.plantinglife import ExtractImages


class GenerateImagesWithoutHumanOverridesCsv(luigi.Task):
    plants_filename: str = luigi.Parameter()  # type: ignore

    def output(self):
        filename = os.path.basename(self.plants_filename)
        filename_no_ext = os.path.splitext(filename)[0]
        return [
            luigi.LocalTarget(
                f"data/out/images-without-human-overrides-{filename_no_ext}.csv"
            )
        ]

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

        with self.output()[0].open("w") as out:
            csv_out = csv.DictWriter(out, fields)
            csv_out.writeheader()

            for i, scientific_name in enumerate(scientific_names):
                print(
                    f"Processing {scientific_name} ({i + 1} of {len(scientific_names)})"
                )

                # run both, even though flickr is prioritized. image picker
                # will use both as options.
                tasks = [
                    flickr.TransformBestFlickrImage(scientific_name),
                    # inaturalist.TransformBestINaturalistImage(scientific_name),
                ]

                # TODO: Consider moving this up into a `requires`.
                #       I had to increase file descriptor limit with `ulimit -n 2048` for this to run.
                luigi.build(
                    tasks,
                    workers=len(tasks),
                    local_scheduler=True,
                )

                row_out = {"scientific_name": scientific_name.capitalize()}
                for task in tasks:
                    with task.output()[0].open() as f:
                        json_str = f.read().strip()

                        # only use the first image result that is populated
                        if json_str and len(row_out) == 1:
                            print(f"populating for: {scientific_name}")
                            parsed = json.loads(json_str)

                            row_out.update(parsed)

                del tasks

                # the flickr image will have a few fields that shouldn't appear
                # in the csv, so filter the row to just the expected fields
                row_out = {key: row_out[key] for key in row_out if key in fields}

                if len(row_out) == len(fields):
                    csv_out.writerow(row_out)
                else:
                    print(f"Missing data, skipping row for {scientific_name}")


class GenerateImagesCsv(luigi.Task):
    plants_filename: str = luigi.Parameter()  # type: ignore

    def output(self):
        filename = os.path.basename(self.plants_filename)
        filename_no_ext = os.path.splitext(filename)[0]
        return [luigi.LocalTarget(f"data/out/images-{filename_no_ext}.csv")]

    def requires(self):
        return [
            GenerateImagesWithoutHumanOverridesCsv(plants_filename=self.plants_filename)
        ]

    def run(self):
        # originals are data that was automatically gathered
        with self.input()[0][0].open() as csvfile:
            reader = csv.DictReader(csvfile)
            originals = list(reader)

        # human overrides are data that someone defined to override
        # the automatically generated data.  convert to dict for fast lookups.
        human_overrides_filename = "data/in/human-choices/images.csv"
        with open(human_overrides_filename, "r", newline="") as csvfile:
            reader = csv.DictReader(csvfile)
            overrides = {item["scientific_name"]: item for item in reader}

        fields = [
            "scientific_name",
            "title",
            "author",
            "license",
            "original_url",
            "card_url",
        ]

        with self.output()[0].open("w") as out:
            csv_out = csv.DictWriter(out, fields)
            csv_out.writeheader()

            for original in originals:
                scientific_name = original["scientific_name"]
                override = overrides.get(scientific_name, None)

                if override:
                    csv_out.writerow(override)
                else:
                    csv_out.writerow(original)


IMAGE_DB_FIELDS = ["id", "title", "card_url", "original_url", "author", "license"]


class GenerateImagesSql(luigi.Task):
    plants_filename: str = luigi.Parameter()  # type: ignore

    def output(self):
        filename = os.path.basename(self.plants_filename)
        filename_no_ext = os.path.splitext(filename)[0]
        return [luigi.LocalTarget(f"data/out/images-{filename_no_ext}.sql")]

    def requires(self):
        return [
            GenerateImagesCsv(plants_filename=self.plants_filename),
            ExtractImages(),
        ]

    @staticmethod
    def get_updated_fields(old: dict[str, Any], new: dict[str, Any]) -> dict[str, Any]:
        updated_fields = {}
        for field_name, new_value in new.items():
            if field_name not in IMAGE_DB_FIELDS:
                continue

            # this sanitization exists just to make the existing values diff nicely
            # but this sanitization doesn't stick, because the 'new plant' path also
            # needs sanitization without calling this function.
            new_value_sanitized = new_value
            new_value_sanitized = new_value.replace("'", "''")
            new_value_sanitized = emoji.replace_emoji(new_value_sanitized, "")

            old_value = old[field_name].replace("'", "''")
            if field_name == "title" and len(new_value_sanitized) > 190:
                new_value_sanitized = new_value_sanitized[:190] + "..."

            if old_value != new_value_sanitized:
                updated_fields[field_name] = new_value

        return updated_fields

    @staticmethod
    def to_sql_setters(fields: dict[str, Any]) -> str:
        sql = "SET"
        spaces = "   "
        for field_name, value in fields.items():
            if field_name not in IMAGE_DB_FIELDS:
                continue

            escaped = value.replace("'", "''")
            sanitized = emoji.replace_emoji(escaped, "")
            if field_name == "title" and len(sanitized) > 190:
                sanitized = sanitized[:190] + "..."

            sql += f"{spaces}{field_name} = '{sanitized}',\n"

            # just being particular about formatting...
            spaces = "      "

        return sql.rstrip(",\n ")

    def run(self):
        with (
            self.input()[0][0].open() as plant_csv,
            self.input()[1][0].open() as name_to_image_json,
            self.output()[0].open("w") as out,
        ):
            reader = csv.DictReader(plant_csv)
            name_to_image = json.loads(name_to_image_json.read())

            next_generated_id = (
                max([image["id"] for image in name_to_image.values()]) + 1
            )

            for row in reader:
                updated_image = dict(row)

                scientific_name = updated_image["scientific_name"]
                existing_image = name_to_image.get(scientific_name, None)

                if existing_image is None:
                    # an image doesn't exist, so create it and point the plant at it
                    #
                    # TODO: This section can simplify if images stores plant_id, instead of plants storing image_id
                    out.write(
                        f"INSERT INTO images \n{self.to_sql_setters(updated_image)},\n      id={next_generated_id};\n"
                    )

                    out.write(
                        f"UPDATE plants SET image_id = {next_generated_id} WHERE id = (SELECT id FROM plants where scientific_name = '{scientific_name}');\n\n"
                    )
                    next_generated_id += 1
                else:
                    updated_fields = self.get_updated_fields(
                        existing_image, updated_image
                    )

                    if updated_fields:
                        # the image already exists and has updated fields, so update it
                        out.write(
                            f"UPDATE images \n{self.to_sql_setters(updated_fields)}\nWHERE id = {existing_image['id']};\n\n"
                        )
