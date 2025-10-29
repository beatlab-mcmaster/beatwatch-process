import pandas as pd
from .parsers import Parser, select_period
from .visualize import vis_single_ts, vis_save

import holoviews as hv
import hvplot.pandas


def main() -> None:
    ### Temporary testing

    # Initialize parser with timezone
    parser = Parser(timezone="America/Toronto")

    df = parser.parse_file("tests/data/03-01_time_21-13-42_a6ed_W023.hr")
    # df = parser.parse_file("../../tests/data/03-02_time_01-53-40_f937_W025.sv")
    print(df)
    print(df["data_hr"].info())

    t1 = pd.to_datetime("2025-03-01 16:14:00-05:00")
    t2 = pd.to_datetime("2025-03-01 16:15:00-05:00")
    d = pd.to_timedelta("0.5s")

    p1 = select_period(df, time_start=t1, time_end=t2, duration=d)
    # print(p1)

    p2 = select_period(
        df,
        time_start=pd.to_timedelta("19.3s"),
        duration=d,
        time_column_name="time_elapsed",
    )
    # print(p2)

    p3 = select_period(df, time_start=t1, time_end=t2, duration=d)
    # print(p3)

    # vis_single_ts(df["data_hr"], "ppg_raw")
    fig = vis_single_ts(df["data_hr"], "ppg_raw", x="time_elapsed")

    vis_save(fig, "test.html")
