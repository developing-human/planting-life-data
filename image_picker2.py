from os import environ

# this snippet, from a github issue, helps find the tcl/tkinter
# library when running through uv.
if not ("TCL_LIBRARY" in environ and "TK_LIBRARY" in environ):
    import platform
    import tkinter
    from pathlib import Path
    from sys import base_prefix

    try:
        tkinter.Tk()
    except tkinter.TclError:
        tk_dir = "tcl" if platform.system() == "Windows" else "lib"
        tk_path = Path(base_prefix) / tk_dir
        environ["TCL_LIBRARY"] = str(next(tk_path.glob("tcl8.*")))
        environ["TK_LIBRARY"] = str(next(tk_path.glob("tk8.*")))

import FreeSimpleGUI as sg

IMG_SIZE = 300
GRID_WIDTH = 6
GRID_HEIGHT = 17
NUM_IMAGES = GRID_WIDTH * GRID_HEIGHT
CHOICES_CSV_FILENAME = "data/in/human-choices/images.csv"


def create_window() -> sg.Window:
    # TODO: left sidebar will need a button per scientific name in input file
    left_sidebar = [
        [
            sg.Button(
                f"Left Sidebar {n}", font=("Helvetica", 12, "bold"), key=f"-SIDEBAR-{n}"
            )
        ]
        for n in range(1, 10)
    ]

    # create a button for each potential image, these will be toggled to visible
    # when they have an image to show, but otherwise will stay invisible.
    image_grid = [
        [
            sg.Button(
                f"Img {row * GRID_WIDTH + col + 1}",
                size=(10, 3),
                key=f"-IMAGE-{row * GRID_WIDTH + col}",
                visible=False,
            )
            for col in range(GRID_WIDTH)
        ]
        for row in range(GRID_HEIGHT)
    ]

    right_column = [
        [
            sg.Column(
                [
                    [sg.Text("Common Name:"), sg.Text("", key="-COMMON-NAME-")],
                    [
                        sg.Text("Scientific Name:"),
                        sg.Text("", key="-SCIENTIFIC-NAME-"),
                    ],
                ],
                size=(None, 300),
                expand_x=True,
                key="-INFO-AREA-",
            )
        ],
        [
            sg.Column(
                image_grid,
                expand_y=True,
                expand_x=True,
                scrollable=True,
                key="-IMAGE-AREA-",
            )
        ],
    ]
    layout = [
        [
            sg.Column(
                left_sidebar,
                size=(None, 2000),
                scrollable=True,
                vertical_scroll_only=True,
                pad=(1, 1),
                background_color="green",
            ),
            sg.Column(
                right_column, expand_y=True, expand_x=True, background_color="blue"
            ),
        ]
    ]

    return sg.Window(
        "Image Picker v2", layout, resizable=True, size=(1200, 800), finalize=True
    )


def select_plant(scientific_name: str):
    pass


def select_image(scientific_name: str, image: dict):
    pass


window = create_window()

while True:
    event, values = window.read()  # type: ignore
    if event == sg.WIN_CLOSED:
        break

    if event.startswith("-SIDEBAR-"):
        item_id = int(event.split("-")[2])

        info_area = window["-INFO-AREA-"]
        if info_area is not None:
            info_area.update(visible=False)
            window["-COMMON-NAME-"].update(value=f"common name {item_id}")  # type: ignore
            window["-SCIENTIFIC-NAME-"].update(value=f"Scientific Name {item_id}")  # type: ignore
            info_area.update(visible=True)

        images_for_area = item_id * 3 + 5
        for n in range(0, NUM_IMAGES):
            image = window[f"-IMAGE-{n}"]
            if image is not None:
                image.update(visible=n < images_for_area)

window.close()
