import pytest
from beatwatch_process.parsers import Parser


@pytest.fixture
def parser():
    return Parser("America/Toronto")


def test_parse_file_hraccel_complete(parser):
    res = parser.parse_file("tests/data/251009_hr_accel_complete.csv")
    # Check that required keys are present
    assert "metadata" in res, "'metadata' key should always be in result"
    assert "data_hr" in res, "'data_hr' key should be in result"
    assert "data_accel" in res, "'data_accel' key should be in result"
    # Verify correct shape of dataframes
    assert len(res["data_hr"].shape) == (70440, 6), (
        "Shape of heart rate dataframe is incorrect"
    )
    assert len(res["data_accel"].shape) == (36762, 7), (
        "Shape of accel dataframe is incorrect"
    )
