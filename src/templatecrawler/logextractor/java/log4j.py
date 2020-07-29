from pathlib import Path
from typing import Union
import re
import pandas

from ..extractorbase import ExtractorBase


class log4jExtractor(ExtractorBase):

    log_statement_0 = re.compile(r'(fatal|info|error|debug|trace|warn|log|printf)\(')
    log_statement_1 = re.compile(r'\.log\(')

    bracket = re.compile(r'\(|\)')

    def extract_events(self):
        self._log_statements = []
        self._log_statement_files = []
        last_dir = self._path.name

        for _file in self._path.rglob('*.java'):
            with open(_file, 'r') as fd:
                strip_parents = _file.parts[_file.parts.index(last_dir) + 1:]
                filename = '/'.join(strip_parents)
                try:
                    data = fd.read()
                    search_result = [m.end() for m in re.finditer(self.log_statement_0, data)]
                    for index_end in search_result:
                        self._log_statements.append(self._parse_log_statement(data[index_end:]))
                        self._log_statement_files.append(filename)
                except UnicodeDecodeError:
                    pass
                except ValueError:
                    pass

    def get_event_count(self):
        return len(self._log_statements)

    def save(self, path: Union[str, Path], repo_name: str, repo_url: str):
        assert(len(self._log_statement_files) == len(self._log_statements))
        entries = len(self._log_statements)
        df = pandas.DataFrame({'print': self._log_statements, 'file': self._log_statement_files,
                               'name': [repo_name] * entries, 'url': [repo_url] * entries})
        df.to_csv(path)

    def _parse_log_statement(self, parameters: str):
        index = 0
        stack = ['(']

        while index < len(parameters):
            match = re.search(self.bracket, parameters[index:])
            if not match:
                raise ValueError("Unexpected EOF")
            if parameters[index + match.start()] == '(':
                stack.append('(')
            elif parameters[index + match.start()] == ')':
                stack.pop()

            index += match.end()
            if not stack:
                return parameters[:index - 1]
        raise ValueError("Unexpected EOF")
