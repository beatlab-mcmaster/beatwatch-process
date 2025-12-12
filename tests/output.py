import os
import pandas as pd
from beatwatch_process.parsers import Parser, select_period
from beatwatch_process.visualize import vis_single_ts, vis_save
from beatwatch_process.utils import log, get_valid_watch_files, load_config
from beatwatch_process.process import upsample

import holoviews as hv
import hvplot.pandas

log.info("================= Starting analysis =====================")

# Configation values
cfg = load_config("tests/test_config.yml")


os.makedirs(cfg["dir_output"], exist_ok=True)

files = get_valid_watch_files(cfg["dir_data"])

# Initialize parser with timezone
parser = Parser(cfg["timezone"])

df = parser.parse_file(cfg["dir_data"] + "251009_hr_accel_complete.csv")
print(df)
print(df["data_hr"].shape)
print(df["data_accel"].shape)
print(df["data_hr"].info())
print(df["data_accel"].info())

# df = parser.parse_file("../../tests/data/03-02_time_01-53-40_f937_W025.sv")

# df_1k = upsample(df["data_hr"])
#
# print(df_1k.head())
#
#
# t1 = pd.to_datetime("2025-03-01 16:14:00-05:00")
# t2 = pd.to_datetime("2025-03-01 16:15:00-05:00")
# d = pd.to_timedelta("0.5s")
#
# p1 = select_period(df, time_start=t1, time_end=t2, duration=d)
# # print(p1)
#
# p2 = select_period(
#     df,
#     time_start=pd.to_timedelta("19.3s"),
#     duration=d,
#     time_column_name="time_elapsed",
# )
# print(p2)

# p3 = select_period(df, time_start=t1, time_end=t2, duration=d)
# print(p3)

# vis_single_ts(df["data_hr"], "ppg_raw")
# fig = vis_single_ts(df["data_hr"], "ppg_raw", x="time_elapsed")
#
# vis_save(fig, cfg["dir_output"] + "fig_01.html")
