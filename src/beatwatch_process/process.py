import pandas as pd
from beatwatch_process.utils import log


def upsample(
    df: pd.DataFrame, max_gap: int = 150, upsample_rate: int = 1
) -> pd.DataFrame:
    """Return dataframe with a new datetimeindex at the specified `upsample_rate`.
    Points between original values (up to `max_gap`) are linearly interpolated.
    A new column is added with identified 'gaps'."""
    df_out = df.copy()
    if not isinstance(df_out.index, pd.DatetimeIndex):
        if "time_absolute" in df_out.columns:
            df_out.set_index("time_absolute", inplace=True)
        else:
            log.error("Could not determine datetimeindex column")
            raise Exception("Could not determine datetimeindex column")

    # Create new time index
    time = pd.date_range(
        start=df_out.index.min(),
        end=df_out.index.max(),
        freq=pd.to_timedelta(upsample_rate, unit="ms"),
    )
    # Determine if there are gaps in data; first get difference between samples
    df_out["diff"] = df_out.index.diff().total_seconds() * 1000
    # Flag if difference greater than max_gap
    df_out["gap"] = df_out["diff"] >= max_gap
    # Apply new index
    df_out = df_out.reindex(time)

    # Do not interpolate above max_gap
    df_out[df_out.select_dtypes(include="number").columns] = df_out.select_dtypes(
        include="number"
    ).interpolate(limit=max_gap)
    df_out["gap"] = df_out["gap"].astype("boolean").ffill()
    if df_out["gap"].any():
        log.info(f"Missing periods (>= {max_gap}) found in data")
        log.warning(f"Missing periods (>= {max_gap}) found in data")
    return df_out.drop(columns=["diff"])
