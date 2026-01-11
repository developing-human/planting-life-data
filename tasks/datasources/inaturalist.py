import json
import time
from typing import Any

import luigi
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry


class ExtractINaturalistSearchResults(luigi.Task):
    """Searches iNaturalist for the given plant. Returns iNaturalist's
    raw response from observation search."""

    scientific_name: str = luigi.Parameter()  # type: ignore

    def output(self):
        # Sanitize by converting to lowercase, swapping spaces for hyphens,
        # and only keeping letters/hyphens
        sanitized = sanitize_search_term(self.scientific_name)
        return [luigi.LocalTarget(f"data/raw/inaturalist/{sanitized}.json")]

    def run(self):
        print("extract from iNaturalist (api call)")

        params = {
            "quality_grade": "research",
            "photos": "true",
            "per_page": 50,
            "q": self.scientific_name,
            "search_on": "name",
            "photo_licensed": "true",
            "photo_license": "cc-by,cc-by-nc,cc-by-nd,cc-by-sa,cc-by-nc-nd,cc-by-nc-sa,cc0",
            "order_by": "votes",
        }

        retries = Retry(
            total=3, backoff_factor=1, status_forcelist=[429, 500, 502, 503, 504]
        )

        session = requests.Session()
        session.mount("https://api.inaturalist.org", HTTPAdapter(max_retries=retries))

        response = session.get(
            "https://api.inaturalist.org/v1/observations", params=params, timeout=10
        )
        time.sleep(2)

        with self.output()[0].open("w") as f:
            pretty_json = json.dumps(json.loads(response.text), indent=4)
            f.write(pretty_json)


LICENSE_ID_TO_NAME = {
    "cc-by": "CC BY 4.0",
    "cc-by-nc": "CC BY-NC 4.0",
    "cc-by-nc-nd": "CC BY-NC-ND 4.0",
    "cc-by-nc-sa": "CC BY-NC-SA 4.0",
    "cc-by-nd": "CC BY-ND 4.0",
    "cc-by-sa": "CC BY-SA 4.0",
    "cc0": "CC0",
}


class TransformValidINaturalistImages(luigi.Task):
    """Transforms Flickr's raw result into a list of photos into a simplified
    model with only the fields that are needed.  Discards invalid data."""

    scientific_name: str = luigi.Parameter()  # type: ignore

    def requires(self):
        return [ExtractINaturalistSearchResults(scientific_name=self.scientific_name)]

    def output(self):
        # Sanitize by converting to lowercase, swapping spaces for hyphens,
        # and only keeping letters/hyphens
        sanitized = sanitize_search_term(self.scientific_name)

        return [
            luigi.LocalTarget(
                f"data/transformed/inaturalist-sanitized/{sanitized}.json"
            )
        ]

    def run(self):
        with self.input()[0][0].open("r") as inaturalist_result:
            inaturalist_result_json = json.loads(inaturalist_result.read())

        # Extract the useful parts from loaded json to prevent dealing with
        # this while filtering/prioritizing
        results = inaturalist_result_json.get("results", [])

        all_transformed = []
        for result in results:
            for observation_photo in result.get("observation_photos", []):
                photo = observation_photo.get("photo")

                transformed = self._transform_inaturalist_photo(result, photo)

                if transformed is not None:
                    all_transformed.append(transformed)

        with self.output()[0].open("w") as f:
            f.write(json.dumps(all_transformed, indent=4))

    def _transform_inaturalist_photo(self, result: Any, photo: Any) -> dict | None:
        id = result.get("id")
        faves = int(result.get("faves_count", 0))
        url = photo.get("url").replace("/square.", "/medium.")
        user = result.get("user", {})

        if photo["license_code"] is None:
            return None

        return {
            "author": user.get("name") or user.get("login"),
            "views": faves,  # close enough :)
            # iNaturalist doesn't really do titles/descriptions
            "title": self.scientific_name,
            "description": "",
            "original_url": f"https://www.inaturalist.org/observations/{id}",
            "card_url": url,
            "height": photo.get("original_dimensions", {}).get("height", 1),
            "width": photo.get("original_dimensions", {}).get("width", 1),
            # fails if missing/unexpected license
            "license": LICENSE_ID_TO_NAME[photo["license_code"]],
        }


#
class TransformBestINaturalistImage(luigi.Task):
    """Takes the top inaturalist image."""

    scientific_name: str = luigi.Parameter()  # type: ignore

    def requires(self):
        return [
            TransformValidINaturalistImages(scientific_name=self.scientific_name),
        ]

    def output(self):
        return [
            luigi.LocalTarget(
                f"data/transformed/inaturalist/image/{self.scientific_name}.json"
            )
        ]

    def run(self):
        inputs = self.input()
        with inputs[0][0].open("r") as photos:
            photos_json = json.loads(photos.read())

        if photos_json:
            best_image = photos_json[0]
        else:
            best_image = {}

        with self.output()[0].open("w") as f:
            f.write(json.dumps(best_image, indent=4))


def sanitize_search_term(search_term: str) -> str:
    "Given a search term, sanitizes it so it can be used in a filename"

    sanitized = search_term.lower().replace(" ", "-")
    sanitized = "".join(ch for ch in sanitized if ch.isalpha() or ch == "-")

    return sanitized
