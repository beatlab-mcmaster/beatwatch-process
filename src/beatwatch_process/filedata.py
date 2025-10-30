import pandas as pd
from typing import TypedDict, NotRequired


class FileData(TypedDict):
    metadata: dict
    data_hr: NotRequired[pd.DataFrame]
    data_accel: NotRequired[pd.DataFrame]
    data_survey: NotRequired[pd.DataFrame]
