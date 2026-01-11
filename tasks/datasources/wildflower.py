import json
import time

import luigi
import requests
from bs4 import BeautifulSoup

import tasks.datasources.usda.usda as usda
from tasks.lenient import LenientTask, StrictError

SOURCE_NAME = "Wildflower"


class ExtractWildflowerHtml(LenientTask):
    """Extracts HTML from wildflower.org/plants for a single plant.

    Input: scientific name of plant (genus + species)
    Output: raw html (*.html) and url (*.source.txt)
    """

    scientific_name: str = luigi.Parameter()  # type: ignore

    def requires(self):
        # Wildflower.org uses USDA symbols in its URL
        return [usda.TransformSymbol(scientific_name=self.scientific_name)]

    def output(self):
        return [
            luigi.LocalTarget(f"data/raw/wildflower/{self.scientific_name}.html"),
            luigi.LocalTarget(f"data/raw/wildflower/{self.scientific_name}.source.txt"),
        ]

    def run_lenient(self):
        print("extract from wildflower (api call)")

        with self.input()[0].open() as f:
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
    scientific_name: str = luigi.Parameter()  # type: ignore

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
            wf_moistures = soup.find("strong", string="Soil Moisture:")

            if wf_moistures:
                wf_moistures = wf_moistures.next_sibling.strip()

            # Convert wildflower moistures to our data format
            result = {}
            if wf_moistures:
                result["low_moisture"] = "Dry" in wf_moistures
                result["medium_moisture"] = "Moist" in wf_moistures
                result["high_moisture"] = "Wet" in wf_moistures
                result["moisture_source"] = SOURCE_NAME
                result["moisture_source_detail"] = source_detail.read()

                remaining = (
                    wf_moistures.replace("Dry", "")
                    .replace("Moist", "")
                    .replace("Wet", "")
                    .replace(",", "")
                    .strip()
                )

                if remaining != "":
                    raise StrictError(f"Unexpected moisture: {remaining}")

            # Write formatted JSON, for easier troubleshooting
            with self.output().open("w") as f:
                if result:
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
    scientific_name: str = luigi.Parameter()  # type: ignore

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


# WARNING: As of 2025-11-30, this is not used, as USDA's data was preferred.
#          Keeping this around in case USDA has holes or this ends up better.
class TransformHabit(LenientTask):
    """Parses plant's habit (tree, shrub, etc) out of wildflower.org HTML.

    Input: scientific name of plant (genus + species)
    Output: String (enum?), of the plants habit
            One of: tree, shrub, grass, garden

    If file does not exist or parsing fails, writes a blank output file.
    """

    task_namespace = "wildflower"  # allows tasks of same name in diff packages
    scientific_name: str = luigi.Parameter()  # type: ignore

    def requires(self):  # type: ignore
        return ExtractWildflowerHtml(scientific_name=self.scientific_name)

    def output(self):  # type: ignore
        return luigi.LocalTarget(
            f"data/transformed/wildflower/habit/{self.scientific_name}.json"
        )

    def run_lenient(self):
        with self.input()[0].open() as content, self.input()[1].open() as source_detail:
            soup = BeautifulSoup(content, "html.parser")

            # Find "Habit" in the html, habits are in next element
            habit_tag = soup.find("strong", string="Habit:")

            result = {}
            if habit_tag:
                next_sibling = habit_tag.find_next_sibling()
                wf_habit_lower = next_sibling.get_text().strip().lower()

                if "tree" in wf_habit_lower:
                    result["habit"] = "tree"
                elif "shrub" in wf_habit_lower:
                    result["habit"] = "shrub"
                elif "grass" in wf_habit_lower:
                    result["habit"] = "grass"
                else:
                    result["habit"] = "garden"

                result["habit_source"] = SOURCE_NAME
                result["habit_source_detail"] = source_detail.read()

            # Write formatted JSON, for easier troubleshooting
            with self.output().open("w") as f:  # type: ignore
                f.write(json.dumps(result, indent=4))
