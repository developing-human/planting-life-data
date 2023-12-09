import luigi
import requests
import time
from tasks.lenient import LenientTask
from bs4 import BeautifulSoup
import tasks.datasources.usda as usda
import json

SOURCE_NAME = "Wildflower"


class ExtractWildflowerHtml(LenientTask):
    """Extracts HTML from wildflower.org/plants for a single plant.

    Input: scientific name of plant (genus + species)
    Output: raw html (*.html) and url (*.source.txt)
    """

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

        # Fetch HTML from wildflower
        url = f"https://www.wildflower.org/plants/result.php?id_plant={symbol}"
        response = requests.get(
            url,
            # Setting the user agent is necessary for it to give a good response
            headers={
                "User-Agent": "Mozilla/5.0 (X11; Linux x86_64; rv:109.0) Gecko/20100101 Firefox/118.0",
            },
            # When a plant is missing, wildflower.org redirects to main page.
            # This shows up as a 302/Found, but we need to disallow redirects
            # if we want to see it.  Without this we see 200/OK.
            allow_redirects=False,
        )

        # Throttle, to prevent spamming their service
        time.sleep(2)

        # Plants which aren't found tend to return a 302
        # This shouldn't fail the job, so ValueError is raised instead of StrictError
        if response.status_code != 200:
            raise ValueError(f"Wildflower returned {response.status_code} for {symbol}")

        # Write the fetched HTML
        with self.output()[0].open("w") as f:
            f.write(response.text)

        # Write the URL where it was fetched from
        with self.output()[1].open("w") as f:
            f.write(url)


class TransformMoisture(LenientTask):
    """Parses plant moisture preferences out of wildflower.org HTML.

    Input: scientific name of plant (genus + species)
    Output: JSON object with:
        low_moisture: Will this plant grow in low moisture?
        medium_moisture: Will this plant grow in medium moisture?
        high_moisture: Will this plant grow in high moisture?
        moisture_source: Always set to "Wildflower"
        moisture_source_detail: The wildflower.org url where this data was found

    If file does not exist or parsing fails, writes a blank output file.
    """

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

            # Find "Soil Moisture:" in the html, moistures are in next element
            wf_moistures = soup.find(
                "strong", string="Soil Moisture:"
            ).next_sibling.strip()

            # Convert wildflower moistures to our data format
            result = {}
            if wf_moistures:
                result["low_moisture"] = "Dry" in wf_moistures
                result["medium_moisture"] = "Moist" in wf_moistures
                result["high_moisture"] = "Wet" in wf_moistures
                result["moisture_source"] = SOURCE_NAME
                result["moisture_source_detail"] = source_detail.read()

            # Write formatted JSON, for easier troubleshooting
            with self.output().open("w") as f:
                f.write(json.dumps(result, indent=4))


class TransformShade(LenientTask):
    """Parses plant shade preferences out of wildflower.org HTML.

    Input: scientific name of plant (genus + species)
    Output: JSON object with:
        full_sun: Will this plant grow in full sun?
        part_shade: Will this plant grow in part shade?
        full_shade: Will this plant grow in full shade?
        shade_source: Always set to "Wildflower"
        shade_source_detail: The wildflower.org url where this data was found

    If file does not exist or parsing fails, writes a blank output file.
    """

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

            # Find "Light Requirement" in the html, shades are in next element
            wf_shades = soup.find(
                "strong", string="Light Requirement:"
            ).next_sibling.strip()

            # Because "Shade" is a substring of "Part Shade" wf_shades is
            # split into a list before converting to our format
            wf_shades = [s.strip() for s in wf_shades.split(",")]

            # Convert wildflower shades to our data format
            result = {}
            if wf_shades:
                result["full_sun"] = "Sun" in wf_shades
                result["part_shade"] = "Part Shade" in wf_shades
                result["full_shade"] = "Shade" in wf_shades
                result["shade_source"] = SOURCE_NAME
                result["shade_source_detail"] = source_detail.read()

            # Write formatted JSON, for easier troubleshooting
            with self.output().open("w") as f:
                f.write(json.dumps(result, indent=4))
