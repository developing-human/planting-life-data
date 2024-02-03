import pytest
import luigi
from luigi.mock import MockTarget
from tasks.datasources.wildflower import TransformMoisture
from tasks.datasources.chatgpt import TransformPollinatorRating
from tasks.lenient import StrictError


@pytest.fixture
def rating_task():
    task = TransformPollinatorRating(scientific_name="test_name")
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
