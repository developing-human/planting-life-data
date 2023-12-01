import luigi
import requests
import time
from tasks.lenient import LenientTask, StrictError
from bs4 import BeautifulSoup
import tasks.datasources.usda as usda


class ExtractWildflowerHtml(LenientTask):
    scientific_name: str = luigi.Parameter()

    def requires(self):
        # Wildflower.org uses USDA symbols in its URL
        return usda.TransformSymbol(scientific_name=self.scientific_name)

    def output(self):
        return luigi.LocalTarget(f"data/raw/wildflower/{self.scientific_name}.html")

    def run_lenient(self):
        symbol = self.input().open().read().strip()

        response = requests.get(
            f"https://www.wildflower.org/plants/result.php?id_plant={symbol}",
            headers={
                "User-Agent": "Mozilla/5.0 (X11; Linux x86_64; rv:109.0) Gecko/20100101 Firefox/118.0",
            },
            allow_redirects=False,
        )

        # Throttle, to prevent spamming their service
        time.sleep(2)

        if response.status_code != 200:
            raise ValueError(f"Wildflower returned {response.status_code} for {symbol}")

        with self.output().open("w") as f:
            f.write(response.text)


class TransformMoisture(LenientTask):
    scientific_name: str = luigi.Parameter()

    def requires(self):
        return ExtractWildflowerHtml(scientific_name=self.scientific_name)

    def output(self):
        return luigi.LocalTarget(
            f"data/transformed/wildflower/moisture/{self.scientific_name}.txt"
        )

    def run_lenient(self):
        with self.input().open("r") as f:
            soup = BeautifulSoup(f, "html.parser")

            wf_moistures = (
                soup.find("strong", string="Soil Moisture:")
                .next_sibling.strip()
                .split(",")
            )

            pl_moistures = []
            if any(wf_moistures):
                for wf_moisture in wf_moistures:
                    match wf_moisture.strip():
                        case "Dry":
                            pl_moistures.append("None")
                        case "Moist":
                            pl_moistures.append("Some")
                        case "Wet":
                            pl_moistures.append("Lots")
                        case _:
                            raise StrictError("Unexpected moisture: {wf_moisture}")

            with self.output().open("w") as f:
                f.write(",".join(pl_moistures))


class TransformShade(LenientTask):
    scientific_name: str = luigi.Parameter()

    def requires(self):
        return ExtractWildflowerHtml(scientific_name=self.scientific_name)

    def output(self):
        return luigi.LocalTarget(f"data/transformed/wildflower/shade/{self.scientific_name}.txt")

    def run_lenient(self):
        with self.input().open("r") as f:
            soup = BeautifulSoup(f, "html.parser")

            wf_moistures = (
                soup.find("strong", string="Light Requirement:")
                .next_sibling.strip()
                .split(",")
            )

            pl_moistures = []
            if any(wf_moistures):
                for wf_moisture in wf_moistures:
                    match wf_moisture.strip():
                        case "Sun":
                            pl_moistures.append("None")
                        case "Part Shade":
                            pl_moistures.append("Some")
                        case "Shade":
                            pl_moistures.append("Lots")
                        case _:
                            raise StrictError(f"Unexpected shade: {wf_moisture}")

            with self.output().open("w") as f:
                f.write(",".join(pl_moistures))
