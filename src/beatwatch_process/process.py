import pandas as pd

from beatwatch_process.filedata import FileData
from beatwatch_process.utils import log


def upsample(
    df: pd.DataFrame,
    max_gap: int = 150,
    upsample_rate: int = 1,
    output_rate: int = 1,
    time_start: pd.Timestamp | None = None,
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
    df_out[df_out.select_dtypes(include="number").columns] = (
        df_out.select_dtypes(include="number").interpolate(limit=max_gap)
    )
    df_out["gap"] = df_out["gap"].astype("boolean").ffill()
    if df_out["gap"].any():
        log.warning(
            f"Missing periods {sum(df_out['gap'])} (>= {max_gap}) found in data"
        )

    # Filter output timeseries to output_rate
    if output_rate > 1:
        if time_start is None:
            log.warning(
                "No start time provided to align grids across devices! "
                "(device timestamps are out of phase)"
            )

        else:
            if time_start > df_out.index.max():
                log.error(
                    "Cannot align output with start time provided "
                    "(time_start is after end of input data frame)"
                )
                raise ValueError("Invalid time_start")

            # Construct aligned 1 ms grid starting at time_start
            full_index = pd.date_range(
                start=time_start,
                end=df_out.index.max(),
                freq=pd.to_timedelta(upsample_rate, unit="ms"),
            )

            # Reindex: insert NaN rows automatically before original data
            df_out = df_out.reindex(full_index)

            # Now downsample on the aligned grid
            df_out = df_out.iloc[::output_rate]

    return df_out.drop(columns=["diff"])


def time_resolved_stats(data: FileData) -> pd.DataFrame:
    """Compute mean, sd, <and ?> across single devices"""
    df = pd.DataFrame()
    return df
