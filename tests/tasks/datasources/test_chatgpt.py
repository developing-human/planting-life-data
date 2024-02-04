import pytest
from tasks.datasources.chatgpt import TransformPollinatorRating
from tasks.datasources.chatgpt import TransformSize
from tasks.datasources.chatgpt import TransformBloom


@pytest.fixture
def rating_task():
    task = TransformPollinatorRating(scientific_name="test_name")
    return task


@pytest.fixture
def size_task():
    task = TransformSize(scientific_name="test_name")
    return task


@pytest.fixture
def bloom_task():
    task = TransformBloom(scientific_name="test_name")
    return task


def test_lowercase_labeled_rating(rating_task: TransformPollinatorRating):
    rating = rating_task.parse_rating(
        """blah blah blah
rating: 8
blah blah blah""",
    )
    assert rating == 8


def test_lowercase_labeled_rating_with_indent(rating_task: TransformPollinatorRating):
    rating = rating_task.parse_rating(
        """blah blah blah
            rating: 8
            blah blah blah""",
    )
    assert rating == 8


def test_uppercase_labeled_rating(rating_task: TransformPollinatorRating):
    rating = rating_task.parse_rating(
        """blah blah blah
Rating: 8
blah blah blah""",
    )
    assert rating == 8


def test_uppercase_labeled_rating_no_space(rating_task: TransformPollinatorRating):
    rating = rating_task.parse_rating(
        """blah blah blah
Rating:8
blah blah blah""",
    )
    assert rating == 8


def test_unlabeled_rating(rating_task: TransformPollinatorRating):
    rating = rating_task.parse_rating(
        """Threadleaf coreopsis is a...

In terms of ...can be rated as a 7 on a scale from 1 to 10. While it is not...""",
    )
    assert rating == 7


def test_unlabeled_rating_2(rating_task: TransformPollinatorRating):
    rating = rating_task.parse_rating(
        """Threadleaf coreopsis is a...

In terms of ...can be rated 7 out of 10. While it is not...""",
    )
    assert rating == 7


def test_unlabeled_rating_3(rating_task: TransformPollinatorRating):
    rating = rating_task.parse_rating(
        """Threadleaf coreopsis is a...

In terms of ...can be rated 7/10. While it is not...""",
    )
    assert rating == 7


def test_unlabeled_rating_4(rating_task: TransformPollinatorRating):
    rating = rating_task.parse_rating(
        """Threadleaf coreopsis is a...

I would rate blah blah as a 6. While it is not...""",
    )
    assert rating == 6


def test_size_inches(size_task: TransformSize):
    size = size_task.parse_size(
        """Threadleaf coreopsis typically grows to a height of...
18"-24\""""
    )
    assert size == '18"-24"'


def test_size_feet(size_task: TransformSize):
    size = size_task.parse_size(
        """Threadleaf coreopsis typically grows to a height of...
2'-3'"""
    )
    assert size == "2'-3'"


def test_size_whitespace_at_end(size_task: TransformSize):
    size = size_task.parse_size(
        """Threadleaf coreopsis typically grows to a height of...
18"-24"   
    """
    )
    assert size == '18"-24"'


def test_size_first_unit_missing(size_task: TransformSize):
    size = size_task.parse_size(
        """Threadleaf coreopsis typically grows to a height of...
18-24\""""
    )
    assert size == '18-24"'


def test_size_last_unit_missing(size_task: TransformSize):
    with pytest.raises(ValueError):
        size_task.parse_size(
            """Threadleaf coreopsis typically grows to a height of...
18"-24"""
        )


def test_bloom_check_every_expected_season(bloom_task: TransformBloom):
    tests = {
        "early spring": "early spring",
        "late spring": "late spring",
        "spring": "spring",
        "early summer": "early summer",
        "late summer": "late summer",
        "summer": "summer",
        "early fall": "early fall",
        "late fall": "late fall",
        "fall": "fall",
        "early autumn": "early fall",
        "late autumn": "late fall",
        "autumn": "fall",
        "does not bloom": "N/A",
    }

    for season_in, season_out in tests.items():
        bloom = bloom_task.parse_bloom(
            f"Threadleaf coreopsis typically blooms in {season_in}"
        )
        assert bloom == season_out


def test_bloom_no_expected_season(bloom_task: TransformBloom):
    with pytest.raises(ValueError):
        bloom_task.parse_bloom(
            "Threadleaf coreopsis typically blooms during the apocalypse"
        )
