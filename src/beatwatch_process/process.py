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
    """Upsample a datetime-indexed dataframe and interpolate short gaps.

    Parameters
    ----------
    max_gap:
        Maximum original sampling gap, in milliseconds, that may be
        interpolated.
    upsample_rate:
        Intermediate grid period, in milliseconds.
    output_rate:
        Keep every n-th sample from the intermediate grid.
    time_start:
        Start time used to align the output grid.
    """
    if df.empty:
        log.warning("Empty dataframe!")  # TODO: handle this better
        return df.copy()

    df_out = df.copy()

    if not isinstance(df_out.index, pd.DatetimeIndex):
        if "time_absolute" not in df_out.columns:
            raise ValueError("Could not determine datetime index column")
        df_out.set_index("time_absolute", inplace=True)

    if not df_out.index.is_monotonic_increasing:
        df_out = df_out.sort_index()

    if df_out.index.has_duplicates:
        log.warning("Duplicate timestamps found; keeping the first value")
        df_out = df_out[~df_out.index.duplicated(keep="first")]

    if upsample_rate <= 0:
        raise ValueError("upsample_rate must be greater than zero")

    if output_rate <= 0:
        raise ValueError("output_rate must be greater than zero")

    if max_gap < 0:
        raise ValueError("max_gap must not be negative")

    period = pd.to_timedelta(upsample_rate, unit="ms")
    original_index = df_out.index

    # Mark original gaps before reindexing.
    gap = (
        original_index.to_series()
        .diff()
        .gt(pd.to_timedelta(max_gap, unit="ms"))
    )

    # Include original timestamps so interpolation uses the real samples.
    grid = pd.date_range(
        start=original_index.min(),
        end=original_index.max(),
        freq=period,
        tz=original_index.tz,
    )
    full_index = grid.union(original_index).sort_values()

    df_out = df_out.reindex(full_index)

    # Interpolate only numeric columns, using elapsed time.
    numeric_cols = df_out.select_dtypes(include="float64").columns

    df_out[numeric_cols] = df_out[numeric_cols].interpolate(
        method="time",
        limit_area="inside",
    )

    # Propagate the original gap flag onto the expanded index.
    df_out["_gap"] = gap.reindex(full_index).ffill().fillna(False).astype(bool)

    if df_out["_gap"].any():
        log.warning(
            "Missing periods %d (>= %d ms) found in data",
            int(df_out["_gap"].sum()),
            max_gap,
        )

    # Align to a phase-locked grid if requested.
    if output_rate > 1:
        if time_start is None:
            log.warning(
                "No start time provided to align grids across devices; "
                "timestamps may be out of phase."
            )
        else:
            if time_start > original_index.max():
                raise ValueError(
                    "time_start is after the end of the input dataframe"
                )

            aligned_index = pd.date_range(
                start=time_start,
                end=original_index.max(),
                freq=period,
                tz=original_index.tz,
            )

            # Keep timestamps from the aligned grid only.
            df_out = df_out.reindex(aligned_index)

        df_out = df_out.iloc[::output_rate]

    # Compute elapsed time for new index
    df_out["time_elapsed"] = df_out.index - df_out.index[0]

    return df_out


def time_resolved_stats(data: FileData) -> pd.DataFrame:
    """Compute mean, sd, <and ?> across single devices"""
    df = pd.DataFrame()
    return df
