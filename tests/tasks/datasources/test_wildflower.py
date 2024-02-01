import pytest
import luigi
import json
from luigi.mock import MockTarget
from tasks.datasources.wildflower import TransformMoisture
from tasks.lenient import StrictError


@pytest.fixture
def moisture_task():
    task = TransformMoisture(scientific_name="test_name")
    task.input = lambda: [MockTarget("input"), MockTarget("input")]
    task.output = lambda: MockTarget("output")
    return task


def test_all_values(moisture_task):
    result = run_task(
        moisture_task, "<strong>Soil Moisture:</strong>Dry , Moist , Wet <br/>"
    )

    result_json = json.loads(result)
    assert result_json["low_moisture"] == True
    assert result_json["medium_moisture"] == True
    assert result_json["high_moisture"] == True


def test_one_values(moisture_task):
    result = run_task(moisture_task, "<strong>Soil Moisture:</strong>Dry<br/>")

    result_json = json.loads(result)
    assert result_json["low_moisture"] == True
    assert result_json["medium_moisture"] == False
    assert result_json["high_moisture"] == False


def test_no_soil_moisture(moisture_task):
    result = run_task(moisture_task, "")
    assert result == ""


def test_invalid_soil_moisture(moisture_task):
    with pytest.raises(StrictError):
        run_task(moisture_task, "<strong>Soil Moisture:</strong>Damp<br/>")


def run_task(task: luigi.Task, input: str) -> str:
    with task.input()[0].open("w") as f:
        f.write(input)

    # with inputs[1].open("w") as f:
    # f.write("a source url")

    task.run()

    with task.output().open("r") as f:
        return f.read()
