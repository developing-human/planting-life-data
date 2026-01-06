import csv
import json
import sys
import typing
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from io import BytesIO
from itertools import zip_longest
from operator import itemgetter

import FreeSimpleGUI as sg
import luigi
import requests
from PIL import Image, ImageDraw

from tasks.datasources.flickr import TransformPrioritizedFlickrImages
from tasks.datasources.inaturalist import TransformValidINaturalistImages
from tasks.datasources.usda.usda import TransformCommonName, TransformHabit

IMG_SIZE = 300
GRID_WIDTH = 6
GRID_HEIGHT = 17
NUM_IMAGES = GRID_WIDTH * GRID_HEIGHT
CHOICES_CSV_FILENAME = "data/in/human-choices/images.csv"

current_index = 0
current_scientific_name = ""


@dataclass
class Plant:
    scientific_name: str
    common_name: str | None
    habit: str | None
    # images: list[dict] = []

    def to_str(self) -> str:
        result = self.scientific_name

        if self.common_name:
            result += f"\n{self.common_name}"

        if self.habit:
            result += f"\n{self.habit}"

        return result

    def max_len(self) -> int:
        lens = [len(self.scientific_name)]
        if self.common_name:
            lens.append(len(self.common_name))
        if self.habit:
            lens.append(len(self.habit))

        return max(lens)


# Defines the layout of the Window and tracks context needed to run the application.
class ImagePickerWindow(sg.Window):
    plants: list[Plant]
    current_plant_index: int
    current_images: list[dict]

    def __init__(self, plants: list[Plant]):
        longest_name = max([plant.max_len() for plant in plants])
        layout = [
            [
                # left sidebar, for selecting which plant to show
                sg.Column(
                    [
                        [
                            sg.pin(
                                sg.Button(
                                    plant.to_str(),
                                    size=(longest_name, 3),
                                    key=f"-SIDEBAR-{idx}",
                                ),
                                shrink=True,
                            )
                        ]
                        for idx, plant in enumerate(plants)
                    ],
                    size=(None, 2000),
                    scrollable=True,
                    vertical_scroll_only=True,
                ),
                # image grid displays images to choose from
                sg.Column(
                    # create a button for each potential image, these will be toggled to visible
                    # when they have an image to show, but otherwise will stay invisible.
                    [
                        [
                            sg.Button(
                                f"Img {row * GRID_WIDTH + col + 1}",
                                key=f"-IMAGE-{row * GRID_WIDTH + col}",
                                visible=False,
                                image_size=(IMG_SIZE, IMG_SIZE),
                            )
                            for col in range(GRID_WIDTH)
                        ]
                        for row in range(GRID_HEIGHT)
                    ],
                    expand_y=True,
                    expand_x=True,
                    scrollable=True,
                    vertical_scroll_only=True,
                    key="-IMAGE-AREA-",
                ),
            ]
        ]

        self.plants = plants
        self.current_plant_index = 0

        super().__init__(
            "Image Picker v2",
            layout,
            resizable=True,
            size=(1200, 800),
            finalize=True,
            font=("Helvetia", 16),
        )

    def current_plant(self) -> Plant:
        return self.plants[self.current_plant_index]


def get_plants(filename_or_scientific_name: str) -> list[Plant]:
    is_filename = filename_or_scientific_name.endswith(".txt")

    print("getting plants")
    if is_filename:
        with open(filename_or_scientific_name, "r") as f:
            scientific_names = f.read().splitlines()

        # Don't prompt for plants which already have images picked
        with open(CHOICES_CSV_FILENAME, "r") as f:
            rows = list(csv.DictReader(f))
            already_chosen_names = [row["scientific_name"].lower() for row in rows]
            scientific_names = [
                name for name in scientific_names if name not in already_chosen_names
            ]
    else:
        # For a specific plant name, always ask for the picture without
        # checking if it was already chosen
        scientific_names = [filename_or_scientific_name.lower()]

    plants = [
        Plant(
            scientific_name,
            get_common_name(scientific_name),
            get_habit(scientific_name),
        )
        for scientific_name in scientific_names
    ]

    print("got plants")
    return plants


def run_luigi_task(task: luigi.Task) -> str | None:
    result = luigi.build(
        [task],
        workers=1,
        local_scheduler=True,
        log_level="WARNING",
    )

    if not result:
        return None

    output = task.output()

    if isinstance(output, list):
        output = output[0]

    return output.open().read()


def get_common_name(scientific_name: str) -> str | None:
    output = run_luigi_task(TransformCommonName(scientific_name=scientific_name))
    if output is None:
        return None

    json_result = json.loads(output)
    return json_result["common_name"]


def get_habit(scientific_name: str) -> str | None:
    output = run_luigi_task(TransformHabit(scientific_name=scientific_name))
    if output is None:
        return None

    json_result = json.loads(output)
    return json_result["habit"]


def load_choices_for_plant(scientific_name: str) -> list[dict]:
    """Given a scientific name, lookup images to choose from based on
    flickr & inaturalist results.

    Output: List of dicts, where each is a *transformed* image"""

    output = run_luigi_task(
        TransformValidINaturalistImages(scientific_name=scientific_name)
    )

    inaturalist_choices = json.loads(output) if output else []

    flickr_choices = []
    search_terms = [
        scientific_name + " blooming",
        scientific_name,
    ]
    for search_term in search_terms:
        output = run_luigi_task(
            TransformPrioritizedFlickrImages(
                scientific_name=scientific_name, search_term=search_term
            )
        )
        if output:
            flickr_choices.extend(json.loads(output))

    # alternate between inaturalist and flickr files
    choices = []
    for left, right in zip_longest(inaturalist_choices, flickr_choices):
        if left is not None:
            choices.append(left)
        if right is not None:
            choices.append(right)

    return choices


def load_image(url: str) -> Image.Image | None:
    """Loads one image from its url and resizes it so the longest side
    is IMG_SIZE."""
    response: requests.Response = requests.get(url)

    if response.status_code != 200:
        print(f"could not fetch image for {url}, status code: {response.status_code}")
        return None

    img = Image.open(BytesIO(response.content))

    if img.width < IMG_SIZE or img.height < IMG_SIZE:
        new_width = img.width
        new_height = img.height
        if img.width > img.height:
            new_width = IMG_SIZE * (img.width / img.height)
            new_height = IMG_SIZE
        else:
            new_height = IMG_SIZE * (img.height / img.width)
            new_width = IMG_SIZE

        img = img.resize((int(new_width), int(new_height)))

    if img.width > img.height:
        # landscape
        # start at middle width wise, and subtract half the height
        # to get an img.height width square
        left = img.width / 2 - img.height / 2
        right = img.width / 2 + img.height / 2
        top = 0
        bottom = img.height
    else:
        # portrait
        left = 0
        right = img.width
        top = img.height / 2 - img.width / 2
        bottom = img.height / 2 + img.width / 2

    cropped = img.crop((left, top, right, bottom))

    # Resize the image
    cropped.thumbnail((IMG_SIZE, IMG_SIZE))

    source_label = None
    if "flickr" in url:
        source_label = "flickr"
    elif "inaturalist" in url:
        source_label = "inaturalist"

    if source_label:
        draw = ImageDraw.Draw(cropped)
        draw.text((6, IMG_SIZE - 30), source_label, (0, 0, 0), font_size=24)
        draw.text((5, IMG_SIZE - 31), source_label, (255, 255, 255), font_size=24)

    return cropped


def load_images_from_urls(
    urls: list[str],
) -> typing.Generator[tuple[int, Image.Image]]:
    with ThreadPoolExecutor() as executor:
        futures_to_idx = {
            executor.submit(load_image, url): idx for idx, url in enumerate(urls)
        }

        for future in as_completed(futures_to_idx):
            idx = futures_to_idx[future]
            try:
                image = future.result()
                if image is not None:
                    yield (idx, image)
            except Exception as exc:
                print(f"could not load: {idx} due to {exc}")


def save_choice(scientific_name: str, choice: dict):
    fields = [
        "scientific_name",
        "title",
        "author",
        "license",
        "original_url",
        "card_url",
    ]

    # Read the existing csv, choices is list of dicts
    with open(CHOICES_CSV_FILENAME, "r", newline="") as csvfile:
        reader = csv.DictReader(csvfile)
        choices = list(reader)

    # Convert choice into expected row format
    row_to_save = {key: choice[key] for key in choice if key in fields}

    scientific_name = scientific_name.capitalize()
    row_to_save["scientific_name"] = scientific_name

    existing_row = next(
        (row for row in choices if row.get("scientific_name") == scientific_name), None
    )

    if existing_row:
        existing_row.update(row_to_save)
    else:
        choices.append(row_to_save)

    # Keep this file sorted for sanity's sake
    choices.sort(key=itemgetter("scientific_name"))

    with open(CHOICES_CSV_FILENAME, "w", newline="") as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=fields)
        writer.writeheader()
        writer.writerows(choices)


# called when a plant is selected, populates the info & image panes
def select_plant(window: ImagePickerWindow, plant_index: int):
    # deselect previous button
    previous_button: sg.Button = window[f"-SIDEBAR-{window.current_plant_index}"]  # type:ignore
    previous_button.update(button_color=sg.DEFAULT_BUTTON_COLOR)

    # update the current plant
    window.current_plant_index = plant_index

    # select the new button
    current_button: sg.Button = window[f"-SIDEBAR-{plant_index}"]  # type:ignore
    current_button.update(button_color="black")
    window.refresh()

    plant = window.current_plant()

    # hide all images for the previous plant
    for idx in range(0, NUM_IMAGES):
        gui_image: sg.Button = window[f"-IMAGE-{idx}"]  # type: ignore
        gui_image.update(visible=False)

    window.current_images = load_choices_for_plant(plant.scientific_name)
    urls = [choice["card_url"] for choice in window.current_images]

    for idx, image in load_images_from_urls(urls[:NUM_IMAGES]):
        gui_image: sg.Button = window[f"-IMAGE-{idx}"]  # type: ignore

        if gui_image is not None and image is not None:
            bio = BytesIO()
            image.save(bio, format="PNG")
            gui_image.update(image_data=bio.getvalue(), visible=True)

            image_area: sg.Column = window["-IMAGE-AREA-"]  # type: ignore
            image_area.contents_changed()
            window.refresh()


# called when an image is selected, saves the result
def select_image(window: ImagePickerWindow, image_idx: int):
    selected_image = window.current_images[image_idx]
    save_choice(window.current_plant().scientific_name, selected_image)

    if window.current_plant_index == len(window.plants) - 1:
        print("No choices left to make, enjoy your day!")
        exit(0)

    # hide current button
    current_button: sg.Button = window[f"-SIDEBAR-{window.current_plant_index}"]  # type: ignore
    current_button.update(visible=False)

    # hide all images
    for idx in range(0, NUM_IMAGES):
        gui_image: sg.Button = window[f"-IMAGE-{idx}"]  # type: ignore
        gui_image.update(visible=False)

    window.refresh()

    # select next plant
    select_plant(window, window.current_plant_index + 1)


def main(filename: str):
    plants = get_plants(filename)
    if not plants:
        print("No choices left to make, enjoy your day!")
        exit(0)

    window = ImagePickerWindow(plants)

    # start with the first plant selected
    select_plant(window, 0)

    while True:
        event, values = window.read()  # type: ignore
        if event == sg.WIN_CLOSED:
            break

        if event.startswith("-SIDEBAR-"):
            item_id = int(event.split("-")[2])
            select_plant(window, item_id)
        elif event.startswith("-IMAGE-"):
            image_id = int(event.split("-")[2])
            select_image(window, image_id)

    window.close()


if __name__ == "__main__":
    if len(sys.argv) != 2:
        print(f"Usage: uv run {sys.argv[0]} plant_list_filename")
        exit(1)

    filename = sys.argv[1]
    main(filename)
