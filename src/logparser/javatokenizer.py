import re
from typing import Callable

from logparser.strstream import Stream


class JavaTokenizer:

    _whitespace_re = re.compile(r'\s')
    _operator_re = re.compile(r'[+*|^/%=\&\-<>!]')
    _punc_re = re.compile(r'[;,.(){}[\]]')
    _identifier_start_re = re.compile(r'[_$A-Za-z]')
    _identifier_re = re.compile(r'[^.+*|^/%=\&\-<>!;,(){}[\]]')  # Match all those which are not punctuation or ops
    _unary_ops_re = re.compile(r'^(\+|\+\+|-|--|!)$')
    _digit_re = re.compile(r'\d')

    def __init__(self, input: Stream):
        self.input = input
        self._current = None

    def is_unary_ops(self, char: str) -> bool:
        return True if re.match(self._unary_ops_re, char) else False

    def _is_whitespace(self, char: str) -> bool:
        return True if re.search(self._whitespace_re, char) else False

    def _is_op_char(self, char: str) -> bool:
        return True if re.search(self._operator_re, char) else False

    def _is_punc(self, char) -> bool:
        return True if re.search(self._punc_re, char) else False

    def _is_identifier_start(self, char) -> bool:
        return True if re.search(self._identifier_start_re, char) else False

    def _is_identifier(self, char) -> bool:
        return True if re.search(self._identifier_re, char) else False

    def _is_digit(self, char) -> bool:
        return True if re.match(self._digit_re, char) else False

    def _read_next(self):
        self._read_while(self._is_whitespace)
        if self.input.eof():
            return None
        char = self.input.peek()
        if char == '"':
            return 'str', self._read_string()
        if self._is_punc(char):
            return 'punc', self.input.next()
        if self._is_op_char(char):
            return 'op', self._read_while(self._is_op_char)
        if self._is_digit(char):
            return 'num', self._read_while(self._is_digit)
        if self._is_identifier(char):
            return 'var', self._read_while(self._is_identifier)
        self.input.croak(f"Can't handle character {char}")

    def _read_while(self, predicate: Callable[[str], bool]):
        tmp_str = ""
        while not self.input.eof() and predicate(self.input.peek()):
            tmp_str += self.input.next()
        return tmp_str

    def _read_string(self) -> str:
        return self._read_escaped('"')

    def _read_escaped(self, end) -> str:
        escaped = False
        s = str()
        self.input.next()
        while not self.input.eof():
            char = self.input.next()
            if escaped:
                s += char
                escaped = False
            elif char == '\\':
                escaped = True
            elif char == end:
                break
            else:
                s += char
        return s

    def peek(self):
        if not self._current:
            self._current = self._read_next()
        return self._current

    def next(self):
        token = self._current
        self._current = None
        return token or self._read_next()

    def eof(self):
        return self.peek() is None
