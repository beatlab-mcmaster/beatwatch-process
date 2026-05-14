import csv
import datetime as dt
import json
import re

import pandas as pd
import pytz

from beatwatch_process.filedata import FileData
from beatwatch_process.utils import log


def get_utc_now():
    """Return UTC date and time as ISO format"""
    return dt.datetime.now(tz=dt.UTC).isoformat()


def summarise_metadata(data):  # TODO: type
    record = []
    for i in data:
        data[i]["metadata"]["file_name"] = i
        record.append(data[i]["metadata"])
    return pd.DataFrame.from_records(record).set_index("file_name")


class Parser:
    # Heart rate data written by BEATwatch
    cols_hr: dict[str, str] = {
        "time_elapsed": "Int64",
        "heart_rate_bpm": "Int16",
        "confidence": "UInt8",
        "ppg_raw": "Int32",
        "ppg_filter": "Int32",
    }
    # Acceleration data written by BEATwatch
    # TODO: numpy datatypes
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
        self,
        rows: list,
        cols: dict[str, str],
        allow_incomplete_samples: bool,
        timedelta_cols=["time_elapsed"],
    ) -> pd.DataFrame:
        """Create a dataframe from csv rows with provided column names and
        datatypes. By default, 'time_elapsed' is converted to timedelta64."""
        df_out = pd.DataFrame(rows, columns=cols.keys())  # type: ignore
        n_df_full = len(df_out.index)
        # Replace empty strings with NaN (common in CSV-like data)
        df_out = df_out.replace(
            ["", "NaN", "nan", "NULL", "null", "None"], pd.NA
        )
        if not allow_incomplete_samples:
            # Drop rows that contain missing values (before casting)
            df_out = df_out.dropna()
        n_df_na = len(df_out.index)
        n_dropped = n_df_full - n_df_na
        if n_dropped > 0:
            log.warning(f"Dropped {n_dropped} rows due to missing values")
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
            # print(json_objs)
            for i in json_objs:
                if "File" in json_objs[i]:  # File information
                    for k, v in json_objs[i]["File"].items():
                        meta_out[k] = v
                if "Status" in json_objs[i]:  # Record information (new format)
                    for k, v in json_objs[i]["Status"].items():
                        meta_out[f"status_{k}"] = v
                    if json_objs[i]["Status"]["state"] == "START_RECORD":
                        for k, v in json_objs[i]["Record"].items():
                            meta_out[f"start_{k}"] = v
                    elif json_objs[i]["Status"]["state"] == "STOP_RECORD":
                        for k, v in json_objs[i]["Record"].items():
                            meta_out[f"stop_{k}"] = v
                if "Record" in json_objs[i]:  # Record information (old format)
                    if "State" in json_objs[i]["Record"]:
                        if json_objs[i]["Record"]["State"] == "START_RECORD":
                            for k, v in json_objs[i]["Record"].items():
                                meta_out[f"start_{k}"] = v
                        elif json_objs[i]["Record"]["State"] == "STOP_RECORD":
                            for k, v in json_objs[i]["Record"].items():
                                meta_out[f"stop_{k}"] = v
                if "DeviceInfo" in json_objs[i]:
                    for k, v in json_objs[i]["DeviceInfo"].items():
                        meta_out[f"deviceInfo_{k}"] = v
                if "Settings" in json_objs[i]:
                    for k, v in json_objs[i]["Settings"].items():
                        meta_out[f"settings_{k}"] = v
                if "question" in json_objs[i]:  # Survey results
                    rows_survey.append(json_objs[i])
        else:
            log.warning("No metadata")

        # Create survey dataframe
        df_out = pd.DataFrame(rows_survey, columns=self.cols_survey.keys())  # type: ignore
        df_out["time_elapsed"] = 0  # astype will not accept Nas
        df_out = df_out.astype(self.cols_survey)
        df_out["time_absolute"] = pd.to_datetime(
            df_out["timeStamp"], unit="ms", utc=True
        ).dt.tz_convert(self.timezone)
        df_out["time_elapsed"] = df_out[
            "time_absolute"
        ] - self._get_start_timestamp(
            meta_out
        )  # Compute time from start of record (to match hr, accel dataframes)
        df_out.drop(columns="timeStamp", inplace=True)  # no longer needed

        return meta_out, df_out

    def _get_start_timestamp(self, metadata: dict):
        """Return timezone-aware timestamp for start of record"""
        start_timestamp = pd.NaT
        meta_ts = ""
        if "start_UNIXTimeStamp" in metadata:
            meta_ts = metadata["start_UNIXTimeStamp"]
        elif "status_startTimestamp" in metadata:
            meta_ts = metadata["status_startTimestamp"]
        else:
            log.warning("Could not find valid start timestamp!")
        start_timestamp = pd.to_datetime(meta_ts, utc=True).tz_convert(
            self.timezone
        )
        return start_timestamp

    def _process_absolute_timestamps(
        self, metadata: dict, df: pd.DataFrame
    ) -> pd.DataFrame:
        """Get the start timestamp from metadata and add to existing time_elapsed
        timestamps"""

        df["time_absolute"] = df["time_elapsed"] + self._get_start_timestamp(
            metadata
        )

        return df

    def _get_from_log(self, file_name: str, search_pattern):
        pattern = re.compile(search_pattern)
        results = []
        try:
            with open(file_name, "r", encoding="utf-8") as f:
                for n, raw_line in enumerate(f):
                    line = raw_line.strip()
                    if not line:
                        continue
                    result = pattern.search(line)
                    if result:
                        results.append({"line": n, "match": result})
        except FileNotFoundError:
            log.error(f"File {file_name} not found")
        except ValueError:
            log.exception("msg")
        except Exception as e:
            log.exception(f"New error: {e}")

        return results

    def correct_timestamp(
        self,
        log_file: str,
        file_data: FileData,
        offset_ms: int = 100,
    ) -> FileData:
        """If synchronization was not run before recording, we can try to
        correct the timestamps of the watch data be searching the info.log
        file"""
        offset: pd.Timedelta = pd.to_timedelta(offset_ms, unit="ms")
        try:
            device_id = file_data["metadata"]["settings_physicalId"]
            misaligned_time = pd.to_datetime(
                file_data["metadata"]["status_startTimestamp"]
            )
        except KeyError:
            device_id = None
            misaligned_time = None
            log.error("Could not get file metadata")
        log.info(
            f"Getting start record logs for device_id: {device_id} with misaligned start time: {misaligned_time}"
        )
        # Filter only logs matching device_id
        search_pattern = rf"\[(?P<time>.{{23}})\] (?P<log>.*): (?P<device>.{{6}}) \[(?P<id>{device_id})\] (?P<msg>.*)$"
        records_log = self._get_from_log(log_file, search_pattern)
        records_parsed = []
        # Parse log messages
        for i in range(len(records_log)):
            time = pd.to_datetime(
                records_log[i]["match"].group("time")
            ).tz_localize("UTC")  # Keep UTC
            msg = records_log[i]["match"].group("msg")
            status = "NA"
            if "Writing 'startRecord();'" in msg:
                records_parsed.append(
                    {
                        "server_time": time,
                        "message": "Start",
                        "status_time": pd.NaT,
                        "correct": False,
                    }
                )
            elif "Starting record" in msg:
                try:
                    status = records_log[i + 1]["match"].group("msg")
                except IndexError:
                    status_time = pd.NaT
                    log.error("Could not find status message")
                finally:
                    if "UNIXTimeStamp" in status:
                        correct = False
                        obj = json.loads(status)
                        status_time = pd.to_datetime(
                            obj["Record"]["UNIXTimeStamp"]
                        )
                        if status_time == misaligned_time:
                            log.info(
                                f"Found start command and status at log time: {time}"
                            )
                            correct = True
                        records_parsed.append(
                            {
                                "server_time": time,
                                "message": "Status",
                                "status_time": status_time,
                                "correct": correct,
                            }
                        )
        records_df = pd.DataFrame(records_parsed)
        records_df["diff"] = (
            records_df["server_time"] - records_df["status_time"]
        )
        mean_offset = records_df[
            records_df["diff"]
            < pd.to_timedelta(200, unit="ms")  # TODO: Add to configuration file
        ]["diff"].mean()
        offset = mean_offset if mean_offset < offset else offset
        new_start = (
            records_df[records_df["correct"]]["server_time"].iloc[-1] - offset
        )
        log.info(
            f"Using offset: {offset} to correct. New start timestamp: {new_start}"
        )
        correction_from_original = new_start - misaligned_time
        log.info(f"Total correction: {correction_from_original}")
        for k, v in file_data.items():
            if isinstance(v, dict):
                file_data[k]["time_corrected_from_log"] = True
                file_data[k]["status_startTimestamp"] = new_start.isoformat()
            elif isinstance(v, pd.DataFrame):
                if "time_absolute" in v.columns:
                    file_data[k] = v.assign(
                        time_absolute=v["time_absolute"]
                        + correction_from_original
                    )
        return file_data

    def update_metadata(
        self, original_metadata: dict, new_metadata: dict
    ) -> None:
        """Replace or add to existing metadata"""
        for k, v in new_metadata.items():
            if k in original_metadata.keys():
                pass
                # log.info(f"Overwriting {k}: {original_metadata[k]} with {k}: {v}") # May want to log this?
            else:
                pass
                # log.info(f"Adding {k}: {v}")
            original_metadata[k] = v

    def parse_file(
        self,
        file_name: str,
        version: float = 0.1,
        allow_incomplete_samples: bool = False,
    ) -> FileData:
        """Read any file created by the BEATwatch application. Data can include
        either, or a mix of, heart rate, acceleration, or survey responses.
        - version: heart rate files written by BEATwatch < 0.2.0 require extra
        processing step"""

        # For handling missing values
        def _to_int(val, default=-1) -> int:
            try:
                return int(val)
            except (ValueError, TypeError):
                return default

        json_objs = {}  # Store metadata and survey responses
        rows_hr = []  # Heart rate samples
        rows_accel = []  # Acceleration samples
        confidence_errors = 0

        try:
            with open(file_name, "r", encoding="utf-8") as f:
                log.info(f"Reading {file_name}")
                for n, raw_line in enumerate(f):
                    line = raw_line.strip()
                    if not line:
                        continue

                    # Read json objects
                    if line[0] == "{":
                        try:
                            json_objs[n] = json.loads(line)
                        except json.JSONDecodeError:
                            log.warning(f"Error reading line {n}")
                        continue

                    # Try to read csv data
                    row = next(csv.reader([line]))
                    if not row:
                        continue

                    # Check for acceleration sample
                    if row[0].startswith("A") and (
                        len(row) == len(self.cols_accel)
                        or allow_incomplete_samples
                    ):
                        row[0] = row[0].strip("A")
                        rows_accel.append(row)
                    elif row[0].startswith("A") and len(row) != len(
                        self.cols_accel
                    ):
                        log.warning(f"Bad accel row: {row}")

                    # Check for heart rate sample
                    elif row[0][0].isdigit() and (
                        len(row) == len(self.cols_hr)
                        or allow_incomplete_samples
                    ):
                        if version < 0.2:
                            try:
                                row[1] = round(_to_int(row[1]) / 10)  # type: ignore
                            except ValueError:
                                row[1] = ""
                                log.warning("Bad heart rate reading")
                                # TODO: dont drop rows, add flag/nas
                        # Odd issue with one data collection, confidence values randomly dropped for sample
                        #  Handle when filter value is written to confidence..
                        if (_to_int(row[2]) > 100) or (_to_int(row[2]) < 0):
                            confidence_errors += (
                                1  # Track if these errors occur
                            )
                            if len(row) == 4:
                                row[3] = row[2]
                            row[2] = ""
                        rows_hr.append(row)
                    elif row[0][0].isdigit() and len(row) != len(self.cols_hr):
                        log.warning(f"Bad hr row: {row}")
                    else:
                        log.warning(f"Unknown data: {row}")

        except FileNotFoundError:
            log.error(f"File {file_name} not found")
        except ValueError:
            log.exception("msg")
        except Exception as e:
            log.exception(f"New error: {e}")

        if confidence_errors > 0:
            log.warning(f"Confidence values missing: {confidence_errors}")

        # Create heart rate dataframe
        df_hr = self._dataframe_from_list(
            rows_hr, self.cols_hr, allow_incomplete_samples
        )

        # Create acceleration dataframe
        df_accel = self._dataframe_from_list(
            rows_accel, self.cols_accel, allow_incomplete_samples
        )

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
            log.warning(f"Start time is out of range: {df_max}")
        elif t2 < df_min:
            log.warning(f"End time is out of range: {df_min}")

        mask = (df_out[time_column_name] >= t1) & (
            df_out[time_column_name] <= t2
        )

        log.success(f"DataFrame {name} -> {sum(mask)} samples")
        return df_out.loc[mask]

    # Calculate t1 and t2 depending on arguments
    if time_start and time_end:
        if duration:
            log.warning(f"Ignoring duration: {duration}")
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

    log.info(f"Selecting period from {t1} to {t2}")

    if isinstance(data, pd.DataFrame):
        return _select("(single)", data)
    else:  # more than 1 dataframe (of static type: FileData)
        return {k: _select(k, v) for k, v in data.items()}  # type: ignore
