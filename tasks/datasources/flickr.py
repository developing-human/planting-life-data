import functools
import json
import luigi
import os
import requests
from tasks.datasources.usda import TransformCommonName


class ExtractFlickrSearchResults(luigi.Task):
    """Searches Flickr for the given search term.  Returns a Flickr's
    raw response from photo search, which contains details like:
    license, author, title, description, size, url."""

    search_term: str = luigi.Parameter()

    def output(self):
        # Sanitize by converting to lowercase, swapping spaces for hyphens,
        # and only keeping letters/hyphens
        sanitized = self.search_term.lower().replace(" ", "-")
        sanitized = "".join(ch for ch in sanitized if ch.isalpha() or ch == "-")

        return luigi.LocalTarget(f"data/raw/flickr/{sanitized}.json")

    def run(self):
        api_key = os.environ["FLICKR_API_KEY"]  # will error if not defined
        params = {
            "method": "flickr.photos.search",
            "api_key": api_key,
            "text": self.search_term,
            "media": "photos",
            "format": "json",
            "nojsoncallback": "1",
            "extras": "views,url_q,url_z,license,owner_name,description",
            "min_upload_date": "2015-01-01",
            "sort": "relevance",
            # This is everything except "All Rights Reserved"
            # docs here: https://www.flickr.com/services/api/flickr.photos.licenses.getInfo.html
            "license": "1,2,3,4,5,6,7,8,9,10",
        }

        response = requests.get(
            "https://api.flickr.com/services/rest", params=params, timeout=5
        )

        with self.output().open("w") as f:
            pretty_json = json.dumps(json.loads(response.text), indent=4)
            f.write(pretty_json)


class TransformValidFlickrImages(luigi.Task):
    """Transforms Flickr's raw result into a list of photos into a simplified
    model with only the fields that are needed.  Discards invalid data."""

    search_term: str = luigi.Parameter()

    def requires(self):
        return ExtractFlickrSearchResults(search_term=self.search_term)

    def output(self):
        # Sanitize by converting to lowercase, swapping spaces for hyphens,
        # and only keeping letters/hyphens
        sanitized = self.search_term.lower().replace(" ", "-")
        sanitized = "".join(ch for ch in sanitized if ch.isalpha() or ch == "-")

        return luigi.LocalTarget(f"data/transformed/flickr-sanitized/{sanitized}.json")

    def run(self):
        with self.input().open("r") as flickr_result:
            flickr_result_json = json.loads(flickr_result.read())

        # Extract the useful parts from loaded json to prevent dealing with
        # this while filtering/prioritizing
        photos = flickr_result_json.get("photos", {}).get("photo", [])

        filtered = self._filter_to_valid_photos(photos)

        transformed = [self._transform_flickr_photo(img) for img in filtered]

        with self.output().open("w") as f:
            f.write(json.dumps(transformed, indent=4))

    def _filter_to_valid_photos(self, photos: list[dict]) -> list[dict]:
        valid_photos = []

        for photo in photos:
            if not photo.get("url_z"):
                print("Invalid photo: missing url_z")
                continue  # url_z points to the sized photo to use

            try:
                int(photo["views"])
            except ValueError:
                print("Invalid photo: views not an int")
                continue  # views must be an integer

            blocked_photo_ids = [
                "37831198204",  # educational drawing of carex crinita
                "17332010645",  # field of apparently dead goldenrod?
                "43826520262",  # too close up of wild ginger
                "41085999240",  # too close up of wild ginger
                "26596674001",  # too close up of wild ginger
                "37356079394",  # too close up of black eyed susan
            ]
            if photo.get("id") in blocked_photo_ids:
                print("Invalid photo: blocked id")
                continue  # filter out blocked photos

            title = photo.get("title", "")
            if self._has_blocked_word(title):
                print("Invalid photo: blocked word in title")
                continue

            description = photo.get("description", {}).get("_content", "")
            if self._has_blocked_word(description):
                print("Invalid photo: blocked word in description")
                continue

            valid_photos.append(photo)

        return valid_photos

    def _has_blocked_word(self, text: str) -> bool:
        blocked_words = [
            "drawn",
            "illustration",
            "dried wildflowers",
            "illustrated",
        ]

        return any(word in text.lower() for word in blocked_words)

    def _transform_flickr_photo(self, photo: dict) -> dict:
        return {
            "title": photo["title"],
            "description": photo.get("description", {}).get("_content", ""),
            "author": photo.get("ownername", ""),
            "views": int(photo.get("views", "0")),
            "original_url": f"https://www.flickr.com/photos/{photo['owner']}/{photo['id']}",
            "resized_url": photo.get("url_z"),
            "height": photo.get("height_z"),
            "width": photo.get("width_z"),
        }


class TransformBestFlickrImage(luigi.Task):
    scientific_name: str = luigi.Parameter()

    def requires(self):
        blooming_search_term = f"{self.scientific_name} blooming"
        non_blooming_search_term = self.scientific_name
        return [
            TransformValidFlickrImages(search_term=blooming_search_term),
            TransformValidFlickrImages(search_term=non_blooming_search_term),
            TransformCommonName(scientific_name=self.scientific_name),
        ]

    def output(self):
        return luigi.LocalTarget(
            f"data/transformed/flickr/image/{self.scientific_name}.json"
        )

    def run(self):
        inputs = self.input()
        with inputs[0].open("r") as blooming:
            with inputs[1].open("r") as non_blooming:
                with inputs[2].open("r") as common_name:
                    blooming_json = json.loads(blooming.read())
                    non_blooming_json = json.loads(non_blooming.read())
                    common_name_json = json.loads(common_name.read())

        # Extract the useful parts from loaded json to prevent dealing with
        # this while filtering/prioritizing
        # blooming_images = blooming_json["photos"]["photo"]
        # non_blooming_images = non_blooming_json["photos"]["photo"]
        common_name_str = common_name_json["common_name"]

        best_image = self._find_best_overall_image(
            blooming_json, non_blooming_json, common_name_str
        )

        # TODO: Is it ok to not write anything if no results?
        #       I think this will fail the run, and I think that's ok.
        if best_image:
            with self.output().open("w") as f:
                f.write(json.dumps(best_image, indent=4))

    def _find_best_overall_image(
        self, blooming: list[dict], non_blooming: list[dict], common_name: str
    ) -> dict | None:
        """Looks through both search results and finds the "best" image."""

        best_blooming = self._find_best_image(blooming, common_name)
        if best_blooming:
            return best_blooming

        best_non_blooming = self._find_best_image(non_blooming, common_name)
        if best_non_blooming:
            return best_non_blooming

        return None

    def _find_best_image(self, images: list[dict], common_name: str) -> dict | None:
        if not images:
            return None

        prioritized_images = self._prioritize_images(images, common_name)

        return prioritized_images[0]

    def _prioritize_images(self, images: list[dict], common_name: str) -> list[dict]:
        scientific_name_lc = self.scientific_name.lower()
        common_name_lc = common_name.lower()

        def has_name(image: dict) -> bool:
            title_lc = image["title"].lower()
            return scientific_name_lc in title_lc or common_name_lc in title_lc

        def is_landscape(image: dict) -> bool:
            return image["width"] > image["height"]

        sorted_images = sorted(
            images,
            key=lambda img: (
                has_name(img),
                is_landscape(img),
                img["views"],
            ),
            reverse=True,
        )

        return sorted_images
