import pytest
import luigi
from luigi.mock import MockTarget
from tasks.datasources.wildflower import TransformMoisture
from tasks.lenient import StrictError


@pytest.fixture
def moisture_task():
    task = TransformMoisture(scientific_name="test_name")
    task.input = lambda: MockTarget("input")
    task.output = lambda: MockTarget("output")
    return task


def test_all_values(moisture_task):
    result = run_task(
        moisture_task, "<strong>Soil Moisture:</strong>Dry , Moist , Wet <br/>"
    )
    assert result == "None,Some,Lots"


def test_one_values(moisture_task):
    result = run_task(moisture_task, "<strong>Soil Moisture:</strong>Dry<br/>")
    assert result == "None"


def test_no_soil_moisture(moisture_task):
    result = run_task(moisture_task, "")
    assert result == ""


def test_invalid_soil_moisture(moisture_task):
    with pytest.raises(StrictError):
        run_task(moisture_task, "<strong>Soil Moisture:</strong>Damp<br/>")


def run_task(task: luigi.Task, input: str) -> str:
    with task.input().open("w") as f:
        f.write(input)

    task.run()

    with task.output().open("r") as f:
        return f.read()
