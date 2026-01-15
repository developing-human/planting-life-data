import json

import luigi
import requests

from tasks.lenient import LenientTask


class TransformSourceUrl(LenientTask):
    """Finds a link to Wikipedia's page about a plant.

    Input: scientific name of plant (genus + species)
    Output: A JSON object with:
        "wiki_source": a link to Wikipedia's page for this plant
    """

    scientific_name: str = luigi.Parameter()  # type: ignore

    def output(self):
        return [
            luigi.LocalTarget(
                f"data/transformed/wikipedia/url/{self.scientific_name}.txt"
            )
        ]

    def run(self):
        page_name = self.scientific_name.capitalize().replace(" ", "_")
        url = f"https://en.wikipedia.org/wiki/{page_name}"

        response: requests.Response = requests.head(
            url,
            headers={
                "User-Agent": "Mozilla/5.0 (X11; Linux x86_64; rv:109.0) Gecko/20100101 Firefox/118.0",
            },
        )

        if response.status_code == 200:
            verified_url = url
        else:
            verified_url = ""

        with self.output()[0].open("w") as out:
            out.write(json.dumps({"wiki_source": verified_url}))
