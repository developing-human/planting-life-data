import luigi
import requests
import json
import csv
import time
import logging


class ExtractPlantList(luigi.Task):
    """Fetches USDA's CSV of all plants.

    It has details like USDA symbol, scientific name and common name."""

    def output(self):
        return luigi.LocalTarget("data/raw/usda/plant-complete-list.csv")

    def run(self):
        response = requests.get(
            "https://plants.usda.gov/assets/docs/CompletePLANTSList/plantlst.txt"
        )
        with self.output().open("w") as f:
            f.write(response.text)


class TransformPlantList(luigi.Task):
    """Converts USDA's plant list into a JSON map from scientific name to USDA symbol"""

    def requires(self):
        return ExtractPlantList()

    def output(self):
        return luigi.LocalTarget("data/transformed/usda/plant-complete-list.json")

    def run(self):
        with self.input().open("r") as csv_file:
            rows = list(csv.DictReader(csv_file))

            # Build a map of "genus species" -> (symbol, full scientific name)
            # The map stores the tuple for the shortest full scientific name
            name_to_tuple = {}
            for row in rows:
                full_name = row.get("Scientific Name with Author")

                # I don't know why, but some scientific names start with this character
                # Removing it cleans up the results a bit
                full_name = full_name.replace("\u00c3\u0097", "")

                # Skip single word scientific names (edge case, may not happen)
                words = full_name.split(" ")
                if len(words) < 2:
                    continue

                # Skip plants where second word is uppercase.
                # These are author names on genus-only scientifc names.
                if words[1][0].isupper():
                    continue

                # Use the genus + species, lowercase, as the official name
                sanitized_name = " ".join(words[:2]).lower()
                symbol = row.get("Symbol")

                # Keep this symbol if it is the shortest full_name seen so far
                if sanitized_name not in name_to_tuple:
                    name_to_tuple[sanitized_name] = (symbol, full_name)
                elif len(full_name) < len(name_to_tuple[sanitized_name][1]):
                    name_to_tuple[sanitized_name] = (symbol, full_name)

            # Simplify name->tuple to just name->symbol, this serializes nicer
            sanitized_name_to_symbol = {}
            for sanitized_name, (symbol, full_name) in name_to_tuple.items():
                sanitized_name_to_symbol[sanitized_name] = symbol

            # Write the name->symbol map, sorted by key for readability
            with self.output().open("w") as f:
                f.write(
                    json.dumps(dict(sorted(sanitized_name_to_symbol.items())), indent=4)
                )


class TransformSymbol(luigi.Task):
    """Converts a scientific name into a USDA symbol.

    Input: scientific name of plant (genus + species)
    Output: USDA's symbol for this plant
    """

    scientific_name: str = luigi.Parameter()

    def requires(self):
        return TransformPlantList()

    def output(self):
        return luigi.LocalTarget(
            f"data/transformed/usda/symbols/{self.scientific_name}.txt"
        )

    def run(self):
        with self.input().open("r") as f:
            scientific_name_to_symbol: dict[str, str] = json.load(f)

            # First, try to lookup the scientific name directly.
            symbol = scientific_name_to_symbol.get(self.scientific_name, None)

            # If not found, check if any USDA scientific names start with this name
            # Ran into this with symphyotrichum novae / symphyotrichum novae-angliae
            # This may be slow, but is hopefully uncommon
            if symbol is None:
                logging.warning(
                    f"{self.scientific_name} not found in map, scanning for prefix"
                )
                for key, value in scientific_name_to_symbol.items():
                    if key.startswith(self.scientific_name):
                        logging.warning(f"Found: {key}")
                        symbol = value
                        break

            # If still not found, this is assumed to be an invalid scientific name.
            # Currently, this means the input has an invalid scientific name.
            # TODO: Should this fail more gracefully?  Currently the script fails.
            if symbol is None:
                raise ValueError(f"Cannot find USDA symbol for: {self.scientific_name}")

            with self.output().open("w") as f:
                f.write(symbol)


class ExtractPlantProfile(luigi.Task):
    """Fetches USDA's plant profile for a single plant.

    The plant profile has information like common name, links to plant guides,
    annual vs perennial, and coarse native status.

    Input: scientific name of plant (genus + species)
    Output: The JSON for the plant profile

    """

    scientific_name: str = luigi.Parameter()

    def requires(self):
        return TransformSymbol(scientific_name=self.scientific_name)

    def output(self):
        return luigi.LocalTarget(
            f"data/raw/usda/plant-profiles/{self.scientific_name}.json"
        )

    def run(self):
        symbol = self.input().open().read().strip()
        response = requests.get(
            f"https://plantsservices.sc.egov.usda.gov/api/PlantProfile?symbol={symbol}"
        )

        # Throttle, to prevent spamming their service
        time.sleep(2)

        with self.output().open("w") as f:
            f.write(response.text)


class TransformCommonName(luigi.Task):
    """Parses a plant's common name out of the USDA Plant Profile.

    Input: scientific name of plant (genus + species)
    Output: A JSON object with:
        "common_name": the plant's common name, formatted in title case
    """

    scientific_name: str = luigi.Parameter()

    def requires(self):
        return ExtractPlantProfile(scientific_name=self.scientific_name)

    def output(self):
        return luigi.LocalTarget(
            f"data/transformed/usda/common-names/{self.scientific_name}.json"
        )

    def run(self):
        with self.input().open("r") as f:
            data = json.load(f)
            # from USDA, capitalization will be inconsistent
            common_name = data["CommonName"]

            # title() will capitalize "gray's" like "Gray'S"
            # this uses it, but then fixes the conjuctions
            sanitized = (
                common_name.title()
                .replace("'S", "'s")
                .replace(" In ", " in ")
                .replace(" The ", " the ")
                .replace(" Of ", " of ")
            )

            result = {"common_name": sanitized}
            with self.output().open("w") as f:
                f.write(json.dumps(result))
