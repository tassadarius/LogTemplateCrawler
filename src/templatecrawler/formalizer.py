import pandas as pd
from templatecrawler.tokentypes import TokenType
from typing import List
import random


def formalize(data: pd.DataFrame, possible_types: List[TokenType]):
    nan_rows = data[data.iloc[:, 0].isnull()]
    data = data.drop(nan_rows.index)
    print(f'Cleaned {nan_rows} empty rows')

    data['preformat'] = data.iloc[:, 0].apply(_parse_string)
    data['formatter_count'] = data['preformat'].apply(_count_formatters)
    data['param_count'] = data.iloc[:, 1].apply(len)
    data = data[data['param_count'] > 0]
    data = data.apply(_cut_longer, axis=1)
    mask = data.apply(lambda row: row['param_count'] == row['formatter_count'], axis=1)
    data = data[mask]

    output = {}
    for i, row in data.iterrows():
        try:
            tmp = _match_tokens(row['preformat'], params=row['arguments'], tokens=possible_types)
            output[i] = tmp
        except (TypeError, ValueError) as e:
            pass
    return output


def _cut_longer(row: pd.Series):
    difference = row['param_count'] > row['formatter_count']
    if 0 < difference < row['param_count']:
        row['arguments'] = row['arguments'][:-difference]
    return row



def _match_tokens(inp: List, params: List[str], tokens: List[TokenType]):
    offsets = []
    for i, token in enumerate(inp):
        if token == '{}':
            offsets.append(i)

    possible_tokens = {}
    for i, param in enumerate(params):
        possible_tokens[param] = []
        param_low = param.lower()
        for token in tokens:
            for feature in token.keywords:
                if param_low.find(feature) >= 0:
                    possible_tokens[param].append(token)
                    break

    for i, param in enumerate(params):
        if possible_tokens[param]:
            token = random.choice(possible_tokens[param])
            inp[offsets[i]] = f'{{{token.name}}}'

    return ' '.join(inp)


def _count_formatters(inp: List[str]) -> int:
    return inp.count('{}')


def _parse_string(inp: str) -> List[str]:
    output = []
    current_tokens = ""
    pos = 0
    for i in range(len(inp)):
        if pos >= len(inp):
            break
        c = inp[pos]
        if c == '{':
            if inp[pos + 1] == '{':
                current_tokens += inp[pos: pos + 1]
                pos += 1
                pass
            elif (offset := _peek(inp[pos + 1:], '}')) > 0:
                if len(current_tokens) > 0:
                    output.append(current_tokens)
                output.append('{}')
                pos += offset + 1   # +1 for normal movement
                current_tokens = ""
            else:
                raise ValueError('{ opened but never closed')
        else:
            current_tokens += c
            pos += 1
    if len(current_tokens) > 0:
        output.append(current_tokens)
    return output


def _peek(inp: str, end: str) -> int:
    for i, c in enumerate(inp):
        if c == end:
            return i + 1
        elif c in {' ', '\n', '\t'}:
            pass
        else:
            print(f'Unexpected token {c} between format brackets {{}}')
    return -1
