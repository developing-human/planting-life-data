import tempfile

from tasks.datasources.usda.location import (
    County,
    NativeLocation,
    Zipcode,
    find_native_zips,
    get_native_counties,
    load_counties,
    load_native_locations,
    load_zipcodes,
)
from tasks.datasources.usda.usda import TransformHabit


def test_habits_tree():
    result = TransformHabit.transform_usda_habits(["Tree"])
    assert result == "tree"


def test_habits_shrub():
    result = TransformHabit.transform_usda_habits(["Shrub"])
    assert result == "shrub"


def test_habits_subshrub():
    result = TransformHabit.transform_usda_habits(["Subshrub"])
    assert result == "garden"


def test_habits_graminoid():
    result = TransformHabit.transform_usda_habits(["Graminoid"])
    assert result == "grass"


def test_habits_forb_herb():
    result = TransformHabit.transform_usda_habits(["Forb/herb"])
    assert result == "garden"


def test_habits_tree_shrub():
    result = TransformHabit.transform_usda_habits(["Tree", "Shrub"])
    assert result == "shrub"

    result = TransformHabit.transform_usda_habits(["Shrub", "Tree"])
    assert result == "shrub"


def test_habits_herb_subshrub():
    result = TransformHabit.transform_usda_habits(["Herb", "Subshrub"])
    assert result == "garden"

    result = TransformHabit.transform_usda_habits(["Subshrub", "Herb"])
    assert result == "garden"


def test_habits_tree_subshrub():
    result = TransformHabit.transform_usda_habits(["Tree", "Forb/herb"])
    assert result == "garden"

    result = TransformHabit.transform_usda_habits(["Forb/herb", "Tree"])
    assert result == "garden"


def test_habits_shrub_subshrub():
    result = TransformHabit.transform_usda_habits(["Shrub", "Subshrub"])
    assert result == "garden"

    result = TransformHabit.transform_usda_habits(["Subshrub", "Shrub"])
    assert result == "garden"


def test_load_counties():
    with tempfile.NamedTemporaryFile() as f:
        f.write(b"""USPS|GEOID|GEOIDFQ|ANSICODE|NAME|ALAND|AWATER|ALAND_SQMI|AWATER_SQMI|INTPTLAT|INTPTLONG
AL|01001|0500000US01001|00161526|Autauga County|1539631460|25677536|594.455|9.914|32.532237|-86.64644
AL|01003|0500000US01003|00161527|Baldwin County|4117933903|1132678359|1589.943|437.33|30.659218|-87.746067""")
        f.flush()

        id_to_county, state_to_counties = load_counties(f.name)

        assert len(id_to_county) == 2
        assert id_to_county["01001"].id == "01001"
        assert id_to_county["01001"].state_fip == "01"
        assert id_to_county["01001"].lat == 32.532237
        assert id_to_county["01001"].lng == -86.64644
        assert id_to_county["01003"].lat == 30.659218
        assert id_to_county["01003"].lng == -87.746067

        assert len(state_to_counties) == 1
        assert len(state_to_counties["01"]) == 2
        assert state_to_counties["01"][0].id == "01001"
        assert state_to_counties["01"][1].id == "01003"


def test_load_zips():
    with tempfile.NamedTemporaryFile() as f:
        f.write(b"""GEOID|GEOIDFQ|ALAND|AWATER|ALAND_SQMI|AWATER_SQMI|INTPTLAT|INTPTLONG
00601|860Z200US00601|166836450|798613|64.416|0.308|18.180555|-66.749961
00602|860Z200US00602|78546196|4428428|30.327|1.71|18.361945|-67.175597""")
        f.flush()

        zips = load_zipcodes(f.name)

        assert len(zips) == 2
        assert zips[0].zipcode == "00601"
        assert zips[0].lat == 18.180555
        assert zips[0].lng == -66.749961
        assert zips[1].zipcode == "00602"
        assert zips[1].lat == 18.361945
        assert zips[1].lng == -67.175597


def test_load_native_locations():
    with tempfile.NamedTemporaryFile() as f:
        f.write(b"""Distribution Data
Symbol,Country,State,State FIP,County,County FIP
ILVE,United States,Alabama,01,,
ILVE,United States,Alabama,01,Autauga,001
ILVE,United States,Alabama,01,Baldwin,003
ILVE,United States,Iowa,19,,
ILVE,Canada,Newfoundland,NF,,
""")
        f.flush()

        locations = load_native_locations(open(f.name, "r"))

        assert len(locations) == 4  # only US locations
        assert locations[0].state_fip == "01"
        assert locations[0].county_fip is None

        assert locations[1].state_fip == "01"
        assert locations[1].county_fip == "001"

        assert locations[2].state_fip == "01"
        assert locations[2].county_fip == "003"

        assert locations[3].state_fip == "19"
        assert locations[3].county_fip is None


def test_get_native_counties():
    all_counties = [
        # included via state w/o county data
        County(id="27080", lat=18, lng=-66, state_fip="27", radius_miles=1),
        County(id="27081", lat=18, lng=-66, state_fip="27", radius_miles=1),
        # included via county-specific data
        County(id="28090", lat=18, lng=-66, state_fip="28", radius_miles=1),
        County(id="28091", lat=18, lng=-66, state_fip="28", radius_miles=1),
        # not included, county-specific data doesn't list county
        County(id="28098", lat=18, lng=-66, state_fip="28", radius_miles=1),
        # not included, state not listed
        County(id="99900", lat=18, lng=-66, state_fip="99", radius_miles=1),
    ]

    native_locations = [
        # state with county data
        NativeLocation(state_fip="28", county_fip=None),
        NativeLocation(state_fip="28", county_fip="090"),
        NativeLocation(state_fip="28", county_fip="091"),
        # county not in county data
        NativeLocation(state_fip="29", county_fip=None),
        NativeLocation(state_fip="29", county_fip="092"),
        # state w/o county data
        NativeLocation(state_fip="27", county_fip=None),
    ]

    id_to_county = {
        "27080": all_counties[0],
        "27081": all_counties[1],
        "28090": all_counties[2],
        "28091": all_counties[3],
        "28098": all_counties[4],
        "99900": all_counties[5],
    }

    state_to_counties = {"27": all_counties[0:2], "28": all_counties[2:5], "29": []}
    native_counties = get_native_counties(
        native_locations, id_to_county, state_to_counties
    )

    assert len(native_counties) == 4
    assert all_counties[0] in native_counties
    assert all_counties[1] in native_counties
    assert all_counties[2] in native_counties
    assert all_counties[3] in native_counties


def test_find_native_zips():
    native_counties = [
        County(id="27080", lat=35, lng=-90, state_fip="27", radius_miles=0),
        County(id="27081", lat=40, lng=-95, state_fip="27", radius_miles=0),
    ]

    # around 35,-90: 0.37 latitude and 0.44 longitude are ~25 miles
    all_zips = [
        # right on top of native county
        Zipcode("11111", lat=35, lng=-90, radius_miles=0),
        # a bit away, but still in range
        Zipcode("22222", lat=35 + 0.35, lng=-90, radius_miles=0),
        Zipcode("33333", lat=35 - 0.35, lng=-90, radius_miles=0),
        Zipcode("44444", lat=35, lng=-90 + 0.4, radius_miles=0),
        Zipcode("55555", lat=35, lng=-90 - 0.4, radius_miles=0),
        # slightly out of range
        Zipcode("66666", lat=35 + 0.4, lng=-90, radius_miles=0),
        Zipcode("77777", lat=35 - 0.4, lng=-90, radius_miles=0),
        Zipcode("88888", lat=35, lng=-90 - 0.5, radius_miles=0),
        Zipcode("99999", lat=35, lng=-90 + 0.5, radius_miles=0),
    ]

    native_zips = find_native_zips(all_zips, native_counties)
    assert len(native_zips) == 5
    assert native_zips == all_zips[0:5]
