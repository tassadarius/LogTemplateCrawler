import re
import pandas as pd
from typing import Union, List

re_length = re.compile(r'^.{10,}$')
re_characters = re.compile(r'( *? [a-zA-Z] *?)')
re_char = re.compile('[a-zA-Z]')


def find_valid(data: Union[pd.Series, List[str]]) -> pd.Series:
    if isinstance(data, List):
        data = pd.Series(data)

    mask = data.str.count(re_length).astype(bool)
    mask |= data.str.count(re_char) > 20
    return mask
