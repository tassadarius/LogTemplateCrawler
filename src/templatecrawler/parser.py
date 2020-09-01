from typing import List, Union
import pandas as pd

from templatecrawler.logparser.java import JavaParser


class LogParser:

    JAVA = 'java'
    PYTHON = 'python'
    CSHARP = 'csharp'

    _engine_selector = {'java': JavaParser,
                        'c': JavaParser,
                        'python': NotImplementedError,
                        'csharp': NotImplementedError}

    def __init__(self, language: str):
        self.language = language
        self._engine = self._engine_selector[language]

    def run(self, raw_input: Union[List[str], pd.Series], framework: str):
        engine = self._engine(framework)
        if isinstance(raw_input, list):
            raw_input = pd.Series(raw_input)
        if isinstance(raw_input, pd.Series):
            return engine.run(raw_input)
        else:
            raise ValueError(f'Expected type <List> or <pandas.Series> got <{type(raw_input)}> instead')
