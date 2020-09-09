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
                line_begin = -1
                try:
                    data = fd.read()
                    search_result = [m.end() for m in re.finditer(self.log_statement_0, data)]
                    for index_end in search_result:
                        line_begin = self._begin_of_line(data, index_end, _file)
                        line_end = self._end_of_line(data, line_begin, filename)
                        self._log_statements.append(data[line_begin:line_end])
                        self._log_statement_files.append(filename)
                except UnicodeDecodeError as e:
                    name = e.__class__.__name__
                    self.logger.info(f'A problem occured parsing {_file}:{line_begin} {name} [Reason] --> {e.reason}')
                except ValueError as e:
                    name = e.__class__.__name__
                    self.logger.info(f'A problem occured parsing {_file}:{line_begin} {name} [Reason] --> {e.args}')
        return self._build_events()

    def get_event_count(self):
        return len(self._log_statements)

    def _build_events(self):
        assert(len(self._log_statement_files) == len(self._log_statements))
        return pandas.DataFrame({'raw': self._log_statements, 'file': self._log_statement_files})

    def save(self, path: Union[str, Path], repo_name: str, repo_url: str):
        assert(len(self._log_statement_files) == len(self._log_statements))
        entries = len(self._log_statements)
        df = pandas.DataFrame({'print': self._log_statements, 'file': self._log_statement_files,
                               'name': [repo_name] * entries, 'url': [repo_url] * entries})
        df.to_csv(path)

    def _begin_of_line(self, data: str, index: int, filename: str = 'unknown') -> int:
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
            if data[index - counter] == ';':
                return self._check_bof_value(index - counter + space_counter + 1, index, filename)
            elif data[index - counter] == '{' or data[index - counter] == '}':
                offset = self._run_forward_comment(data, index - counter)
                return self._check_bof_value(offset, index, filename)
            elif data[index - counter] == '/' and data[index - counter - 1] == '*':
                return self._check_bof_value(index - counter + space_counter + 1, index, filename)
            elif data[index - counter] == '/' and data[index - counter - 1] == '/':
                return self._check_bof_value(self._run_forward_comment(data, index - counter), index, filename)
            elif data[index - counter] == '@':
                return self._check_bof_value(self._run_forward_comment(data, index - counter), index, filename)
            elif data[index - counter] == ':':
                return self._check_bof_value(self._run_forward_comment(data, index - counter), index, filename)
            elif data[index - counter - 1] == '-' and data[index - counter] == '>':
                return self._check_bof_value(index - counter + space_counter + 1, index, filename)
            elif data[index - counter].isspace():
                space_counter += 1
            else:
                space_counter = 0
            counter += 1
        self.logger.info(f'Parsed until file beginning in <{filename}>')
        return 0

    def _check_bof_value(self, bof_index, original_index, filename):
        if original_index - bof_index > 64:
            msg = f'Suspicious high offset in finding the beginning of line (at {filename}:{original_index})'
            self.logger.warning(msg)
        return bof_index


    def _end_of_line(self, data: str, offset: int, file_id: str):
        if file_id not in self._stream.keys():
            self._stream.clear()
            self._stream[file_id] = Stream(data)
        cstream = self._stream[file_id]     # cstream == current stream
        cstream.pos = offset

        while not cstream.eof():
            if cstream.peek() == '"':
                self._read_string(cstream)
            elif cstream.peek() == ';':
                return cstream.pos
            cstream.next()

    def _read_string(self, cstream: Stream):
        escaped = False
        while not cstream.eof():
            if cstream.peek() == r'\\':
                escaped = True
            if cstream.peek() == '"' and not escaped:
                return cstream.pos
        raise ValueError("Unexpected EOF")

    def _run_forward_comment(self, data: str, offset: int):
        i = 0
        while offset + i < len(data):
            i += 1
            chararacter = data[offset + i]
            if chararacter == '\n':
                break

        while offset + i < len(data):
            chararacter = data[offset + i]
            if not chararacter.isspace():
                return offset + i
            i += 1
        raise ValueError('Unexpected EOF')
