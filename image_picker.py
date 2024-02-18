import concurrent.futures
import csv
import json
import PySimpleGUI as sg
import requests
import sys
from PIL import Image
from io import BytesIO
from operator import itemgetter
from tasks.datasources.flickr import sanitize_search_term

IMG_SIZE = 300
GRID_SIZE = 5
NUM_IMAGES = GRID_SIZE * GRID_SIZE
CHOICES_CSV_FILENAME = "data/in/human-choices/images.csv"


def create_window() -> sg.Window:
    layout = [
        [
            sg.Button(
                key=f"image{i + j*GRID_SIZE}",
                image_filename="",
                image_size=(IMG_SIZE, IMG_SIZE),
                pad=(0, 0),
                border_width=0,
            )
            for i in range(GRID_SIZE)
        ]
        for j in range(GRID_SIZE)
    ]

    return sg.Window("Select an Image", layout, finalize=True)


def load_image(url: str) -> Image:
    """Loads one image from its url and resizes it so the longest side
    is IMG_SIZE."""
    response = requests.get(url)
    img = Image.open(BytesIO(response.content))

    if img.width > img.height:
        # landscape
        # start at middle width wise, and subtract half the height
        # to get an img.height width square
        left = img.width / 2 - img.height / 2
        right = img.width / 2 + img.height / 2
        top = 0
        bottom = img.height

        longest_side = img.width
    else:
        # portrait
        left = 0
        right = img.width
        top = img.height / 2 - img.width / 2
        bottom = img.height / 2 + img.width / 2
        
    cropped = img.crop((left, top, right, bottom))

    # Resize the image
    cropped.thumbnail((IMG_SIZE, IMG_SIZE))

    return cropped


def load_images_from_urls(urls: list[str]) -> list[Image]:
    with concurrent.futures.ThreadPoolExecutor() as executor:
        return list(executor.map(load_image, urls))


def load_choices_for_plant(scientific_name: str) -> list[dict]:
    """Given a scientific name, lookup images to choose from based on
    previously prioritized Flickr searches.

    Output: List of dicts, where each is a *transformed* flickr image"""

    base_path = "data/transformed/flickr-prioritized"
    search_terms = [
        scientific_name,
        scientific_name + " blooming",
    ]
    choices_filenames = [
        f"{base_path}/{sanitize_search_term(search_term)}.json"
        for search_term in search_terms
    ]

    choices = []
    for filename in choices_filenames:
        try:
            with open(filename, "r") as f:
                file_json = json.loads(f.read())
                choices.extend(file_json)
        except FileNotFoundError:
            print(f"File not found, skipping: {filename}")

    return choices


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
    row_to_save["scientific_name"] = scientific_name.capitalize()

    choices.append(row_to_save)

    # Keep this file sorted for sanity's sake
    choices.sort(key=itemgetter("scientific_name"))

    with open(CHOICES_CSV_FILENAME, "w", newline="") as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=fields)
        writer.writeheader()
        writer.writerows(choices)


if len(sys.argv) != 2:
    print(f"Usage: python3 {sys.argv[0]} plants_list.txt")
    exit(1)

# Loading here so this can fail before window opens for file not found
plants_filename = sys.argv[1]
with open(plants_filename, "r") as f:
    scientific_names = f.read().splitlines()

# Don't prompt for plants which already have images picked
with open(CHOICES_CSV_FILENAME, "r") as f:
    rows = list(csv.DictReader(f))
    already_chosen_names = [row["scientific_name"].lower() for row in rows]
    scientific_names = [
        name for name in scientific_names if name not in already_chosen_names
    ]

if not scientific_names:
    print("No choices left to make, enjoy your day!")
    exit(0)

window = create_window()

for scientific_name in scientific_names:
    choices = load_choices_for_plant(scientific_name)
    if not choices:
        continue
    
    print(f"Choose for: {scientific_name}")

    urls = [choice["card_url"] for choice in choices]
    images = load_images_from_urls(urls[:NUM_IMAGES])

    # Update the window with the image data
    for i, img in enumerate(images):
        bio = BytesIO()
        img.save(bio, format="PNG")
        window[f"image{i}"].update(image_data=bio.getvalue())

    # This blocks until an image is chosen
    event, values = window.read()
    if event == sg.WINDOW_CLOSED:
        break

    # Clear images after selection is made
    # This fixes a quirk where the last plant's images are shown if this
    # plant doesn't have enough choices
    for i in range(NUM_IMAGES):
        window[f"image{i}"].update(image_data=sg.BLANK_BASE64)

    clicked_index = int(event.split("image")[1])

    choice = choices[clicked_index]
    save_choice(scientific_name, choice)

window.close()
