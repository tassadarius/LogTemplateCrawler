from pathlib import Path
from typing import Union
import re
import pandas
import logging

from ..extractorbase import ExtractorBase
from templatecrawler.logparser.strstream import Stream


class slf4jExtractor(ExtractorBase):
    logger = logging.getLogger(__name__)
    log_statement_0 = re.compile(r'\.(fatal|info|error|debug|trace|warn)\(')
    log_statement_1 = re.compile(r'\.log\(')

    def extract_events(self) -> pandas.DataFrame:
        self._log_statements = []
        self._log_statement_files = []
        last_dir = self._path.name

        for _file in self._path.rglob('*.java'):
            if not _file.is_file():
                continue
            with open(_file, 'r') as fd:
                strip_parents = _file.parts[_file.parts.index(last_dir) + 1:]
                filename = '/'.join(strip_parents)
                try:
                    data = fd.read()
                    search_result = [m.end() for m in re.finditer(self.log_statement_0, data)]
                    for index_end in search_result:
                        line_begin = self._find_log_beginning(data, index_end, _file)
                        line_end = self._end_of_line(data, line_begin)
                        self._log_statements.append(data[line_begin:line_end])
                        self._log_statement_files.append(filename)
                except UnicodeDecodeError as e:
                    self.logger.info(f'A problem occured while parsing {_file}:{line_begin} {e.__class__.__name__}')
                except ValueError as e:
                    self.logger.info(f'A problem occured while parsing {_file}:{line_begin} {e.__class__.__name__}')
        return self._build_events()

    def get_event_count(self):
        return len(self._log_statements)

    def _find_log_beginning(self, data: str, index: int, filename: str = 'unknown') -> int:
        """ After the log event matches, find the beginning of the line, e.g.:
                .info(...);  -->  log.info(...);
            It then returns the index

        :param data: Log file (as string)
        :param index: Given index of a log line
        :param filename: (optional) A filename for logging purposes
        :return: The index of the line start
        """
        space_counter = 0
        counter = 1
        while index - counter > 0:
            if data[index - counter] == '\n':
                if counter - space_counter > 64:
                    msg = f'Suspicious high offset in finding the beginning of line (at {filename}:{index})'
                    self.logger.warning(msg)
                return index - counter + space_counter + 1
            elif data[index - counter].isspace():
                space_counter += 1
            else:
                space_counter = 0
            counter += 1
        if counter - space_counter > 64:
            msg = f'Suspicious high offset in finding the beginning of line (at {filename}:{index})'
            self.logger.warning(msg)
        return 0

    def _build_events(self):
        assert(len(self._log_statement_files) == len(self._log_statements))
        return pandas.DataFrame({'raw': self._log_statements, 'file': self._log_statement_files})

    def save(self, path: Union[str, Path], repo_name: str, repo_url: str):
        assert(len(self._log_statement_files) == len(self._log_statements))
        entries = len(self._log_statements)
        df = pandas.DataFrame({'print': self._log_statements, 'file': self._log_statement_files,
                               'name': [repo_name] * entries, 'url': [repo_url] * entries})
        df.to_csv(path)

    def _end_of_line(self, data: str, offset: int):
        if not self._stream:
            self._stream = Stream(data)
        self._stream.pos = offset

        while not self._stream.eof():
            if self._stream.peek() == '"':
                self._read_string()
            elif self._stream.peek() == ';':
                return self._stream.pos
            self._stream.next()

    def _read_string(self):
        escaped = False
        while not self._stream.eof():
            if self._stream.peek() == r'\\':
                escaped = True
            if self._stream.peek() == '"' and not escaped:
                return self._stream.pos
        raise ValueError("Unexpected EOF")
