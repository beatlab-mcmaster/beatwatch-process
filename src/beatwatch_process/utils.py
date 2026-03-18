import os
import re
import sys
from pathlib import Path

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


def load_config(file_name: str) -> dict:
    """Load the specified configuration file for script"""
    with open(file_name, "r") as file:
        # Get file name (of caller)
        called_by = Path(sys._getframe(1).f_code.co_filename).stem

        # Read configuration file
        config_dat = yaml.safe_load(file)

        # Expand raw data paths if needed
        for k, v in config_dat["paths_in"].items():
            config_dat["paths_in"][k] = Path(
                config_dat["paths_in"][k]
            ).expanduser()
            log.info(f"Input path: {k}: {config_dat['paths_in'][k]}")

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
