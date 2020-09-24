import re
import pandas as pd
from typing import Union, List

re_length = re.compile(r'^.{10,}$')                     # Has to have a certain length
re_char = re.compile('^[^a-wyzA-WYZ]+$')                # Contains no letters, except x for 0x hex escape


def find_valid(data: Union[pd.Series, List[str]]) -> pd.Series:
    if isinstance(data, List):
        data = pd.Series(data)

    mask = data.apply(len) > 12
    mask |= ~data.str.match(re_char)
    return mask
