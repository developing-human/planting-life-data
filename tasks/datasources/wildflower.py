import luigi
import requests
import time
from tasks.lenient import LenientTask
from bs4 import BeautifulSoup
import tasks.datasources.usda as usda
import json

SOURCE_NAME = "Wildflower"


class ExtractWildflowerHtml(LenientTask):
    scientific_name: str = luigi.Parameter()

    def requires(self):
        # Wildflower.org uses USDA symbols in its URL
        return usda.TransformSymbol(scientific_name=self.scientific_name)

    def output(self):
        return [
            luigi.LocalTarget(f"data/raw/wildflower/{self.scientific_name}.html"),
            luigi.LocalTarget(f"data/raw/wildflower/{self.scientific_name}.source.txt"),
        ]

    def run_lenient(self):
        with self.input().open() as f:
            symbol = f.read().strip()

        url = f"https://www.wildflower.org/plants/result.php?id_plant={symbol}"
        response = requests.get(
            url,
            headers={
                "User-Agent": "Mozilla/5.0 (X11; Linux x86_64; rv:109.0) Gecko/20100101 Firefox/118.0",
            },
            allow_redirects=False,
        )

        # Throttle, to prevent spamming their service
        time.sleep(2)

        if response.status_code != 200:
            raise ValueError(f"Wildflower returned {response.status_code} for {symbol}")

        with self.output()[0].open("w") as f:
            f.write(response.text)

        with self.output()[1].open("w") as f:
            f.write(url)


class TransformMoisture(LenientTask):
    task_namespace = "wildflower"  # allows tasks of same name in diff packages
    scientific_name: str = luigi.Parameter()

    def requires(self):
        return ExtractWildflowerHtml(scientific_name=self.scientific_name)

    def output(self):
        return luigi.LocalTarget(
            f"data/transformed/wildflower/moisture/{self.scientific_name}.json"
        )

    def run_lenient(self):
        with self.input()[0].open() as content, self.input()[1].open() as source_detail:
            soup = BeautifulSoup(content, "html.parser")

            wf_moistures = soup.find(
                "strong", string="Soil Moisture:"
            ).next_sibling.strip()

            result = {}
            if wf_moistures:
                result["low_moisture"] = "Dry" in wf_moistures
                result["medium_moisture"] = "Moist" in wf_moistures
                result["high_moisture"] = "Wet" in wf_moistures
                result["moisture_source"] = SOURCE_NAME
                result["moisture_source_detail"] = source_detail.read()

            with self.output().open("w") as f:
                f.write(json.dumps(result, indent=4))


class TransformShade(LenientTask):
    task_namespace = "wildflower"  # allows tasks of same name in diff packages
    scientific_name: str = luigi.Parameter()

    def requires(self):
        return ExtractWildflowerHtml(scientific_name=self.scientific_name)

    def output(self):
        return luigi.LocalTarget(
            f"data/transformed/wildflower/shade/{self.scientific_name}.json"
        )

    def run_lenient(self):
        with self.input()[0].open() as content, self.input()[1].open() as source_detail:
            soup = BeautifulSoup(content, "html.parser")

            wf_shades = (
                soup.find("strong", string="Light Requirement:")
                .next_sibling.strip()
                .split(",")
            )

            wf_shades = [s.strip() for s in wf_shades]

            result = {}
            if wf_shades:
                result["full_sun"] = "Sun" in wf_shades
                result["part_shade"] = "Part Shade" in wf_shades
                result["full_shade"] = "Shade" in wf_shades
                result["shade_source"] = SOURCE_NAME
                result["shade_source_detail"] = source_detail.read()

            with self.output().open("w") as f:
                f.write(json.dumps(result, indent=4))
