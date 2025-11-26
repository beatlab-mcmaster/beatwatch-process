import os
import re
import yaml
import inspect
from pathlib import Path
from beatwatch_process import logging_

log = logging_.setup_logging()


def get_valid_watch_files(data_directory: str):
    match_data = re.compile(r".*(time).*\.(csv|hr|sv)$", re.IGNORECASE)
    file_list = [f for f in os.listdir(data_directory) if match_data.fullmatch(f)]
    log.info(f"Found {len(file_list)} valid files in: {data_directory}")
    return file_list


def load_config(file_name: str) -> dict:
    """Load the specified configuration file for script"""
    with open(file_name, "r") as file:
        # Get file name (of caller)
        called_by = inspect.stack()[1]
        # Read configuration file
        config_dat = yaml.safe_load(file)
        # Add directory for script's results
        config_dat["current_script"] = Path(called_by.filename).name.strip(".py")
        # sub_folders = ["figures", "tables", "processed"]
        # config_dat["paths_out"] = {}
        # for f in sub_folders:
        #     config_dat["paths_out"][f] = Path(
        #         config_dat["dir_results"], config_dat["current_script"], f
        #     )
        log.info(
            f"Analyses will be run with the settings in '{file_name}':"
            + f"\n\n{yaml.dump(config_dat)}",
        )
        return config_dat


def init_directories(config_dat: dict):
    """Create project directories based on configuration file"""
    log.info("Initializing directories:")
    for f, p in config_dat["paths_out"].items():
        log.info(f" - Creating directory: {p}")
        os.makedirs(p, exist_ok=True)


def check_existing(file_name: str):
    """Check for existing file"""
    is_found = False
    if os.path.isfile(file_name):
        log.info(f" - {file_name} already exists, skipping processing")
        is_found = True
    else:
        log.info(f" - {file_name} does not exist, processing...")
    return is_found
