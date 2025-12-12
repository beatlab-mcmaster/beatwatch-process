# Tests

## Initial tests

### Purpose

- Rewriting many code sections:
  - how data are saved on Bangle.js watches
    - acceleration
    - heart rate
    - survey responses
  - modifying data types, sampling rates, writing methods, transfer methods
- Requires different parsing functions
- Regardless of parsing operations, data should be formatted in standardized format

### Tests

Start with `src/beatwatch_process/parsers.py`

#### Unit tests

Straightforward...

#### Integration tests

1. Initialize `Parser`
2. Read raw watch data
3. Assert correct output, errors, warnings

### Raw data files

- `tests/data/251009_hr_accel_complete.csv`
  - File contains heart rate and acceleration data
  - Complete, no errors, no warnings

## TODO

- [ ] Parser should identify missing samples: count, duration, total duration

## Reference

[pytest documentation](https://docs.pytest.org/en/stable/)

- [assertions](https://docs.pytest.org/en/stable/how-to/assert.html)
- [fixtures](https://docs.pytest.org/en/stable/how-to/fixtures.html) for testing classes
