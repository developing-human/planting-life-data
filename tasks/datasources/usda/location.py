import csv
import json
import math
import time
import typing
from collections import defaultdict
from dataclasses import dataclass

import haversine
import luigi
import requests

from tasks.datasources.usda.usda import TransformPlantId

SOURCE_NAME = "USDA"


class ExtractDistributionData(luigi.Task):
    """Fetches USDA's distribution data for a single plant.

    The distribution data says where the plant is native.
    Usually at the county level, but sometimes only at the state level.

    Input: scientific name of plant (genus + species)
    Output: A CSV with state/county location data
    """

    scientific_name: str = luigi.Parameter()  # type: ignore

    def requires(self):  # type: ignore
        return TransformPlantId(scientific_name=self.scientific_name)

    def output(self):  # type: ignore
        return [
            luigi.LocalTarget(
                f"data/raw/usda/distribution-data/{self.scientific_name}.csv"
            ),
        ]

    def run(self):
        id = self.input().open().read().strip()  # type: ignore

        # https://plantsservices.sc.egov.usda.gov/api/PlantProfile/getDownloadDistributionDocumentation -d '{"Field":"Symbol","SortBy":"sortSciName","Offset":null,"MasterId":42478}'
        url = "https://plantsservices.sc.egov.usda.gov/api/PlantProfile/getDownloadDistributionDocumentation"
        response = requests.post(
            url,
            json={
                "Field": "symbol",
                "MasterId": id,
                "Offset": None,
                "SortBy": "sortSciName",
            },
            headers={"Accept": "text/csv"},
        )

        # Throttle, to prevent spamming their service
        time.sleep(2)

        with self.output()[0].open("w") as f:
            f.write(response.text)


@dataclass
class County:
    id: str
    lat: float
    lng: float
    state_fip: str
    radius_miles: float

    @classmethod
    def from_csv_row(cls, row):
        square_miles = float(row["ALAND_SQMI"]) + float(row["AWATER_SQMI"])
        radius_miles = math.sqrt(square_miles / math.pi)

        return cls(
            lat=float(row["INTPTLAT"]),
            lng=float(row["INTPTLONG"]),
            state_fip=row["GEOID"][:2],
            id=row["GEOID"],
            radius_miles=radius_miles,
        )


@dataclass
class Zipcode:
    zipcode: str
    lat: float
    lng: float
    radius_miles: float

    @classmethod
    def from_csv_row(cls, row):
        square_miles = float(row["ALAND_SQMI"]) + float(row["AWATER_SQMI"])
        radius_miles = math.sqrt(square_miles / math.pi)
        return cls(
            lat=float(row["INTPTLAT"]),
            lng=float(row["INTPTLONG"]),
            zipcode=row["GEOID"],
            radius_miles=radius_miles,
        )


@dataclass
class NativeLocation:
    state_fip: str
    county_fip: str | None

    @classmethod
    def from_csv_row(cls, row):
        return cls(
            state_fip=row["State FIP"],
            county_fip=row["County FIP"] if row["County FIP"] != "" else None,
        )


class TransformPlantZipcodes(luigi.Task):
    """Parses a plant's USDA id out of the plant profile.

    Input: scientific name of plant (genus + species)
    Output: The plants usda id
    """

    scientific_name: str = luigi.Parameter()  # type: ignore

    def requires(self):  # type: ignore
        return ExtractDistributionData(scientific_name=self.scientific_name)

    def output(self):  # type: ignore
        return [
            luigi.LocalTarget(
                f"data/transformed/usda/plant-zipcodes/{self.scientific_name}.json"
            ),
            luigi.LocalTarget(
                f"data/transformed/usda/plant-zipcodes/{self.scientific_name}.csv"
            ),
        ]

    def run(self):
        id_to_county, state_to_counties = load_counties(
            "data/in/locations/counties.txt"
        )
        all_zips = load_zipcodes("data/in/locations/zipcodes.txt")
        native_locations = load_native_locations(self.input()[0].open("r"))

        native_counties = get_native_counties(
            native_locations, id_to_county, state_to_counties
        )

        native_zips = find_native_zips(all_zips, native_counties)

        with self.output()[0].open("w") as f:
            f.write(
                json.dumps({"native_zips": [z.zipcode for z in native_zips]}, indent=2)
            )

        native_zips_set = set([z.zipcode for z in native_zips])
        with self.output()[1].open("w") as f:
            f.write("id,point_latitude,point_longitude,found\n")
            for zip in all_zips:
                found = zip.zipcode in native_zips_set

                f.write(f"{zip.zipcode},{zip.lat},{zip.lng},{found}\n")


# load distribution data for this plant, this is a list of counties,
# although some states will lack county specific data.
def load_native_locations(dist_data_csv_reader: typing.TextIO) -> list[NativeLocation]:
    dist_data_csv_reader.readline()  # skip extra header row
    reader = csv.DictReader(dist_data_csv_reader)
    return [
        NativeLocation.from_csv_row(d)
        for d in reader
        if d["Country"] == "United States"
    ]


# load all counties into two maps:
#   id_to_county maps a county's FIP to the County
#   state_to_counties maps a state's FIP to all Counties in that state
def load_counties(filename: str) -> tuple[dict[str, County], dict[str, list[County]]]:
    with open(filename) as counties_csv:
        reader = csv.DictReader(counties_csv, delimiter="|")
        all_counties = [County.from_csv_row(d) for d in reader]

        id_to_county = {county.id: county for county in all_counties}

        state_to_counties = defaultdict(list)
        for county in all_counties:
            state_to_counties[county.state_fip].append(county)

        return id_to_county, state_to_counties


# load latitude and longitude on all zipcodes in the US
def load_zipcodes(filename: str) -> list[Zipcode]:
    with open(filename, "r") as zips_csv:
        reader = csv.DictReader(zips_csv, delimiter="|")
        return [Zipcode.from_csv_row(d) for d in reader]


# resolves the native states/counties into just counties
def get_native_counties(
    native_locations: list[NativeLocation],
    id_to_county: dict[str, County],
    state_to_counties: dict[str, list[County]],
) -> list[County]:
    # map state fips to a list of counties, based on distribution data
    # if a state fip maps to an empty list, that means its county data is missing
    native_state_to_county = {}
    for native_location in native_locations:
        state_fip = native_location.state_fip
        county_fip = native_location.county_fip

        if county_fip is None:
            native_state_to_county[state_fip] = []
        else:
            full_fip = state_fip + county_fip

            # the counties used by USDA don't line up with Gazeteer county
            # data in Connecticut. Report it, but skip them. Below, it will
            # end up using all counties and thats ok since its a small state.
            if full_fip not in id_to_county:
                continue
            native_state_to_county[state_fip].append(id_to_county[full_fip])

    # create a list of all counties where this plant is native
    native_counties = []
    for state, counties in native_state_to_county.items():
        if len(counties) == 0:
            native_counties.extend(state_to_counties[state])
        else:
            native_counties.extend(counties)

    return native_counties


def find_native_zips(
    all_zips: list[Zipcode], native_counties: list[County]
) -> list[Zipcode]:
    native_zips = []

    def inside_bounding_box(zip: Zipcode, county: County, max_dist: float):
        lat_diff = abs(zip.lat - county.lat) * 69
        lng_diff = abs(zip.lng - county.lng) * 69 * math.cos(math.radians(zip.lat))
        return lat_diff <= max_dist and lng_diff <= max_dist

    for zip in all_zips:
        for county in native_counties:
            # extra 25 miles is to be forgiving around what i assume is
            # gaps in county based data from USDA
            max_distance = 25 + zip.radius_miles + county.radius_miles
            if not inside_bounding_box(zip, county, max_distance):
                continue  # not within bounding box, so don't try haversine

            distance = haversine.haversine(
                (zip.lat, zip.lng),
                (county.lat, county.lng),
                unit=haversine.Unit.MILES,
                check=False,
            )

            if distance < max_distance:
                native_zips.append(zip)
                break  # don't check other counties after finding a match

    return native_zips
