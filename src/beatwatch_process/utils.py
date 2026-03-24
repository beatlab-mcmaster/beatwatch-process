import os
import re
import sys
from pathlib import Path

import pandas as pd
import pytz
import yaml

from beatwatch_process import logging_

log = logging_.setup_logging()


def get_valid_watch_files(data_directory: str):
    match_data = re.compile(r".*(time).*\.(csv|hr|sv)$", re.IGNORECASE)
    file_list = [
        f for f in os.listdir(data_directory) if match_data.fullmatch(f)
    ]
    log.success(f"Found {len(file_list)} valid files in: {data_directory}")
    return file_list


def load_config(file_name: str, results_dir="default") -> dict:
    """Load the specified configuration file for script"""
    with open(file_name, "r") as file:
        # 'default' names the result directory the name of the current script
        # This does not work well with jupyter notebooks, so optionally, provide results directory name
        if results_dir == "default":
            # Get file name (of caller)
            called_by = Path(sys._getframe(1).f_code.co_filename).stem
        else:
            called_by = results_dir

        # Read configuration file
        config_dat = yaml.safe_load(file)

        # Expand raw data paths if needed
        for k, v in config_dat["paths_in"].items():
            config_dat["paths_in"][k] = Path(
                config_dat["paths_in"][k]
            ).expanduser()
            log.info(f"Input path: {k}: {config_dat['paths_in'][k]}")

        # Read timezone
        if "timezone" in config_dat:
            log.info(f"Using timestamp: {config_dat['timezone']}")
            config_dat["timezone_pytz"] = pytz.timezone(config_dat["timezone"])

        # Process timestamps
        if "timestamps" in config_dat:
            for k, v in config_dat["timestamps"].items():
                if ("start" in k) or ("end" in k):
                    config_dat["timestamps"][k] = pd.to_datetime(
                        config_dat["timestamps"][k]
                    ).tz_convert(
                        config_dat["timezone_pytz"]
                    )  # TODO: handle no timezone
                    log.info(
                        f"Created timestamp '{k}': {config_dat['timestamps'][k]}"
                    )
                if "length" in k:
                    config_dat["timestamps"][k] = pd.to_timedelta(
                        config_dat["timestamps"][k]
                    )
                    log.info(
                        f"Created timedelta '{k}': {config_dat['timestamps'][k]}"
                    )

        # Add directories for script's results
        sub_folders = ["figures", "tables", "__cache", "summary"]
        config_dat["paths_out"] = {}
        for f in sub_folders:
            config_dat["paths_out"][f] = Path("results", called_by, f)
            log.info(f"Output path: {f}: {config_dat['paths_out'][f]}")
            os.makedirs(config_dat["paths_out"][f], exist_ok=True)
    return config_dat


def check_existing(file_name: str):
    """Check for existing file"""
    is_found = False
    if os.path.isfile(file_name):
        log.info(f" - {file_name} already exists, skipping processing")
        is_found = True
    else:
        log.info(f" - {file_name} does not exist, processing...")
    return is_found
