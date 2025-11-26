def test_parsing():
    from beatwatch_process.parsers import Parser

    parser = Parser()
    data = parser.parse_file("tests/data/03-01_time_21-13-42_a6ed_W023.hr")

    print(data)
    assert data["metadata"]["Name"] == "03-01T21:13:42_a6ed_W023"
    assert data["metadata"]["MAC"] == "cf:55:97:a0:a6:ed"
    assert data["metadata"]["PhysicalID"] == "W023"
    assert data["metadata"]["start_UNIXTimeStamp"] == "2025-03-01T21:13:42.742Z"
    assert data["metadata"]["n_samples_hr"] == 4602
    assert data["metadata"]["n_samples_accel"] == 0
    assert data["metadata"]["n_survey_responses"] == 0
