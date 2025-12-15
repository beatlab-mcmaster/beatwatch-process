from typing import NotRequired, TypedDict

import pandas as pd


class FileData(TypedDict):
    metadata: dict
    data_hr: NotRequired[pd.DataFrame]
    data_accel: NotRequired[pd.DataFrame]
    data_survey: NotRequired[pd.DataFrame]
