import pytest
from beatwatch_process.parsers import Parser


@pytest.fixture
def parser():
    return Parser("America/Toronto")


@pytest.mark.parametrize(
    "test_input,expected",
    [
        ("tests/data/251009_hr_accel_complete.csv", {"md": "metadata"}),
        ("tests/data/251009_hr_accel_errors.csv", {"md": "metadata"}),
    ],
)
def test_parse_file_hraccel_complete(parser, test_input, expected):
    res = parser.parse_file(test_input)
    # Check that required keys are present
    assert expected["md"] in res, "'metadata' key should always be in result"
    assert "data_hr" in res, "'data_hr' key should be in result"
    assert "data_accel" in res, "'data_accel' key should be in result"
    # Verify correct shape of dataframes
    assert res["data_hr"].shape == (70440, 6), (
        "Shape of heart rate dataframe is incorrect"
    )
    assert res["data_accel"].shape == (36762, 7), (
        "Shape of accel dataframe is incorrect"
    )
