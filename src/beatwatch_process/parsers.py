import csv
import json
import pytz
import datetime as dt
import pandas as pd
from .filedata import FileData


def get_utc_now():
    """Return UTC date and time as ISO format"""
    return dt.datetime.now(tz=dt.UTC).isoformat()


class Parser:
    # Heart rate data written by BEATwatch
    cols_hr: dict[str, str] = {
        "time_elapsed": "int64",
        "heart_rate_bpm": "int16",
        "confidence": "UInt8",
        "ppg_raw": "int32",
        "ppg_filter": "int32",
    }
    # Acceleration data written by BEATwatch
    cols_accel: dict[str, str] = {
        "time_elapsed": "int64",
        "x": "int32",
        "y": "int32",
        "z": "int32",
        "magnitude": "int32",
        "difference": "int32",
    }
    # Survey data written by BEATwatch
    cols_survey: dict[str, str] = {
        "number": "int64",
        "item": "int64",
        "timeStamp": "float64",  # Needed to match format of json object
        "question": "category",
        "input": "category",
        "range": "object",
        "response": "object",
        "time_elapsed": "int64",
    }

    def __init__(self, timezone: str = "UTC") -> None:
        """Initialize file parser for BEATwatch data and BEATmonitor server
        logs. Default timestamps are time-aware UTC; optionally configure to
        timezone of records"""
        self.timezone = pytz.timezone(timezone)

    def _dataframe_from_list(
        self, rows: list, cols: dict[str, str], timedelta_cols=["time_elapsed"]
    ) -> pd.DataFrame:
        """Create a dataframe from csv rows with provided column names and
        datatypes. By default, 'time_elapsed' is converted to timedelta64."""
        df_out = pd.DataFrame(rows, columns=cols.keys())  # type: ignore
        n_df_full = len(df_out.index)
        # Replace empty strings with NaN (common in CSV-like data)
        df_out = df_out.replace("", pd.NA)
        # Drop rows that contain missing values (before casting)
        df_out = df_out.dropna()
        n_df_na = len(df_out.index)
        n_dropped = n_df_full - n_df_na
        if n_dropped > 0:
            print(f"WARN: Dropped {n_dropped} rows due to missing values")
        df_out = df_out.astype(cols)
        for c in timedelta_cols:
            df_out[c] = pd.to_timedelta(df_out[c], unit="ms")  # type: ignore
        return df_out

    def _process_json_objs(
        self, json_objs: dict[str, str]
    ) -> tuple[dict[str, str], pd.DataFrame]:
        """Get metadata from metadata objects; return survey data from survey
        responses"""
        meta_out = {
            "Parsed_on": get_utc_now(),
            "StudyName": "NA",
            "StudyInstance": "NA",
        }
        rows_survey = []

        if len(json_objs):
            for i in json_objs:
                if "File" in json_objs[i]:  # File information
                    for k, v in json_objs[i]["File"].items():
                        meta_out[k] = v
                elif "Status" in json_objs[i]:  # Record information (new format)
                    for k, v in json_objs[i]["Status"].items():
                        meta_out[f"status_{k}"] = v
                    if json_objs[i]["Status"]["state"] == "START_RECORD":
                        for k, v in json_objs[i]["Record"].items():
                            meta_out[f"start_{k}"] = v
                    elif json_objs[i]["Status"]["state"] == "STOP_RECORD":
                        for k, v in json_objs[i]["Record"].items():
                            meta_out[f"stop_{k}"] = v
                elif "Record" in json_objs[i]:  # Record information (old format)
                    if json_objs[i]["Record"]["State"] == "START_RECORD":
                        for k, v in json_objs[i]["Record"].items():
                            meta_out[f"start_{k}"] = v
                    elif json_objs[i]["Record"]["State"] == "STOP_RECORD":
                        for k, v in json_objs[i]["Record"].items():
                            meta_out[f"stop_{k}"] = v
                elif "question" in json_objs[i]:  # Survey results
                    rows_survey.append(json_objs[i])
                else:
                    print(f"Unknown object: {json_objs[i]}")

        else:
            print("No metadata")

        # Create survey dataframe
        df_out = pd.DataFrame(rows_survey, columns=self.cols_survey.keys())  # type: ignore
        df_out["time_elapsed"] = 0  # astype will not accept Nas
        df_out = df_out.astype(self.cols_survey)
        df_out["time_absolute"] = pd.to_datetime(
            df_out["timeStamp"], unit="ms", utc=True
        ).dt.tz_convert(self.timezone)
        df_out["time_elapsed"] = df_out["time_absolute"] - self._get_start_timestamp(
            meta_out
        )  # Compute time from start of record (to match hr, accel dataframes)
        df_out.drop(columns="timeStamp", inplace=True)  # no longer needed

        return meta_out, df_out

    def _get_start_timestamp(self, metadata: dict):
        """Return timezone-aware timestamp for start of record"""
        start_timestamp = pd.NaT
        try:
            start_timestamp = pd.to_datetime(
                metadata["start_UNIXTimeStamp"], utc=True
            ).tz_convert(self.timezone)
        except Exception as e:
            # TODO: better handling here
            print(f"Could not find valid start timestamp! {e}")
        return start_timestamp

    def _process_absolute_timestamps(
        self, metadata: dict, df: pd.DataFrame
    ) -> pd.DataFrame:
        """Get the start timestamp from metadata and add to existing time_elapsed
        timestamps"""

        df["time_absolute"] = df["time_elapsed"] + self._get_start_timestamp(metadata)

        return df

    def update_metadata(self, original_metadata: dict, new_metadata: dict) -> None:
        """Replace or add to existing metadata"""
        for k, v in new_metadata.items():
            if k in original_metadata.keys():
                pass
                # print(f"Overwriting {k}: {original_metadata[k]} with {k}: {v}") # May want to log this?
            else:
                pass
                # print(f"Adding {k}: {v}")
            original_metadata[k] = v

    def parse_file(self, file_name: str, version: float = 0.1) -> FileData:
        """Read any file created by the BEATwatch application. Data can include
        either, or a mix of, heart rate, acceleration, or survey responses.
        - version: heart rate files written by BEATwatch < 0.2.0 require extra
        processing step"""
        json_objs = {}  # Store metadata and survey responses
        rows_hr = []  # Heart rate samples
        rows_accel = []  # Acceleration samples

        try:
            with open(file_name, "r", encoding="utf-8") as f:
                print(f"Reading {file_name}")
                for n, raw_line in enumerate(f):
                    line = raw_line.strip()
                    if not line:
                        continue

                    # Read json objects
                    if line[0] == "{":
                        try:
                            json_objs[n] = json.loads(line)
                        except json.JSONDecodeError:
                            print(f"Error reading line {n}")
                        continue

                    # Try to read csv data
                    row = next(csv.reader([line]))
                    if not row:
                        continue

                    # Check for acceleration sample
                    if row[0].startswith("A") and len(row) == len(self.cols_accel):
                        row[0] = row[0].strip("A")
                        rows_accel.append(row)
                    elif row[0].startswith("A") and len(row) != len(self.cols_accel):
                        print(f"Bad accel row: {row}")

                    # Check for heart rate sample
                    elif row[0][0].isdigit() and len(row) == len(self.cols_hr):
                        if version < 0.2:
                            row[1] = round(int(row[1]) / 10)  # type: ignore
                        rows_hr.append(row)
                    elif row[0][0].isdigit() and len(row) != len(self.cols_hr):
                        print(f"Bad hr row: {row}")
                    else:
                        print(f"Unknown data: {row}")

        except FileNotFoundError:
            print(f"File {file_name} not found")
        except Exception as e:
            print(f"Error: {e}")

        # Create heart rate dataframe
        df_hr = self._dataframe_from_list(rows_hr, self.cols_hr)

        # Create acceleration dataframe
        df_accel = self._dataframe_from_list(rows_accel, self.cols_accel)

        # Create metadata, survey dataframe
        meta, df_survey = self._process_json_objs(json_objs)

        # Add absolute timestamps to hr/accel data
        self._process_absolute_timestamps(meta, df_hr)
        self._process_absolute_timestamps(meta, df_accel)

        # Update metadata with results of reading the file
        update = {
            "n_samples_hr": len(df_hr.index),
            "n_samples_accel": len(df_accel.index),
            "n_survey_responses": len(df_survey.index),
            "duration_hr": df_hr["time_elapsed"].max(),
            "duration_accel": df_accel["time_elapsed"].max(),
        }
        self.update_metadata(meta, update)

        # Create FileData structure
        file_data: FileData = {"metadata": meta}
        for name, df in {
            "data_hr": df_hr,
            "data_accel": df_accel,
            "data_survey": df_survey,
        }.items():
            if not df.empty:
                file_data[name] = df

        return file_data

    def extract_raw_files(self, root_directory: str, recursive=True) -> None:
        """Read all files in directory, extract data, write to extracted
        directory"""
        pass

    def parse_log(self):
        # TODO: parse events, drift, syncronization from log files
        pass


def select_period(
    data: pd.DataFrame | FileData,
    time_start: pd.Timestamp | pd.Timedelta | None = None,
    time_end: pd.Timestamp | pd.Timedelta | None = None,
    duration: pd.Timedelta | pd.Timedelta | None = None,
    time_column_name: str = "time_absolute",
) -> pd.DataFrame | FileData:
    """Select data between time_start and time_end timestamps, from time_start
    plus duration, or time_end minus duration to time_end. Default selects
    absolute time periods, use time_column_name='time_elapsed' for periods
    relative to start of record. Note: timestamps should be timezone-aware
    (e.g., '2025-03-01 16:14:00-05:00')"""

    def _select(name, df_in):
        """Return masked dataframe"""
        if not isinstance(df_in, pd.DataFrame):
            return df_in
        df_out = df_in.copy()

        # Check time range of dataframe
        df_min = df_out[time_column_name].min()
        df_max = df_out[time_column_name].max()

        if t1 > df_max:
            print(f"Warning: Start time is out of range: {df_max}")
        elif t2 < df_min:
            print(f"Warning: End time is out of range: {df_min}")

        mask = (df_out[time_column_name] >= t1) & (df_out[time_column_name] <= t2)

        print(f"DataFrame {name} -> {sum(mask)} samples")
        return df_out.loc[mask]

    # Calculate t1 and t2 depending on arguments
    if time_start and time_end:
        if duration:
            print(f"Warning: Ignoring duration: {duration}")
        t1 = time_start
        t2 = time_end
    elif time_start and duration:
        t1 = time_start
        t2 = time_start + duration
    elif time_end and duration:
        t1 = time_end - duration
        t2 = time_end
    else:
        raise ValueError(
            "Two of 'time_start', 'time_end', and 'duration' must be provided"
        )

    print(f"Selecting period from {t1} to {t2}")

    if isinstance(data, pd.DataFrame):
        return _select("(single)", data)
    else:  # more than 1 dataframe (of static type: FileData)
        return {k: _select(k, v) for k, v in data.items()}  # type: ignore
