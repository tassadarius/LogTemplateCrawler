import re
import pandas as pd
from typing import Union, List

re_length = re.compile(r'^.{10,}$')
re_char = re.compile(r'^[^a-wyzA-WYZ]+$')

re_keyword_beginnings = re.compile(r'^\s*(static|#include|#define|#if|#endif)')
re_comment = re.compile(r'^\s*(//|\*)')


def find_valid(data: Union[pd.Series, List[str]]) -> pd.Series:
    if isinstance(data, List):
        data = pd.Series(data)

    mask = data.apply(len) > 14                       # Has to be at least n characters long
    mask |= ~data.str.match(re_char)                  # Has to contain letters
    mask |= ~data.str.match(re_keyword_beginnings)    # Exclude certain keywords, which may come from a parsing problem
    mask |= ~data.str.match(re_comment)               # Exclude comments, which may come from a parsing problem
    mask |= ~ data.str.count('{}') > 12               # Exclude templates with more than 12 placeholders
    return mask
