import holoviews as hv
import pandas as pd

# from .filedata import FileData

hv.extension("bokeh")


def vis_single_ts(df_in: pd.DataFrame, y: str, x: str = "time_absolute"):
    df = df_in.reset_index()
    kdim = hv.Dimension(x, label="Time (absolute)")
    vdim = hv.Dimension(y)  # TODO: optional y label?
    trace = hv.Curve(df, kdim, vdim, group="", label="").opts(
        xrotation=45, width=1000, height=400
    )

    return trace


def vis_multi_ts():
    pass


def vis_save(traces, filename):
    hv.save(traces, filename)
