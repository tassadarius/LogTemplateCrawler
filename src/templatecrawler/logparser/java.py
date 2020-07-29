import pandas as pd
import re
from pathlib import Path
from typing import Union, Tuple, List

from templatecrawler.logparser import filtersettings as fs
from templatecrawler.logparser.strstream import Stream
from templatecrawler.logparser.javatokenizer import JavaTokenizer


class JavaParser:

    #def __init__(self, path_to_csv: Union[str, Path] = None, data: pd.DataFrame = None):
    #    if path_to_csv is None and data is None:
    #        raise ValueError("At least a path_to_csv or directly a DataFrame must be given")
    #    if path_to_csv:
    #        self.df = pd.read_csv(path_to_csv)
    #    if data is not None:
    #        self.df = data
    #    self.df = self.df[self.df['print'].notna()]

    def extract_and_convert(self, data, column='print'):
        #data = self.df[column]

        print(f'Dataset size before filtering is {len(data)}')

        # Filter useless rows
        for current_filter in fs.filter_rules:
            mask = data.apply(lambda x: True if re.search(current_filter, x) else False)  # True if filter matches
            data = data[~mask]                                                            # Remove all Trues
            # self.df = self.df[~mask]                                                      # Also in parent structure
            print(f'Removed {sum(mask)} entries from dataset. New size is {len(data)}')

        output = {'template': [], 'arguments': []}
        for string in data:
            try:
                result, arguments = self._parse(string)
                output['template'].append(result)
                output['arguments'].append(arguments)
            except ValueError as e:
                print(f'{string}\nParsing error: ', e)
        return pd.DataFrame(output)

    def _parse(self, input) -> Tuple[str, List[str]]:
        character_stream = Stream(input)
        lexer = JavaTokenizer(character_stream)

        log_string = ""
        arguments = []
        argument_mode = False
        first_token = True

        # We basically have two modes: Parse string concatenation and parse arguments
        # If we find a String.format we know what to look for. If we don't we assume that the first occurrence of ','
        # is the delimiter between string concatenation and arguments for that string

        while not lexer.eof():
            current_type, current_token = lexer.peek()
            if current_type == 'str':
                if first_token:
                    argument_mode = True
                log_string += current_token
                lexer.next()
            elif current_type == 'op' and current_token == '+':
                lexer.next()
                current_type, current_token = lexer.peek()
                if current_type == 'str':
                    log_string += current_token
                    lexer.next()
                elif current_type == 'op' and not lexer.is_unary_ops(current_token):
                    # print(f'{input} could not be parsed')
                    raise ValueError(f'Operator {current_token} may not follow a +')
                elif current_type == 'op':
                    lexer.next()
                elif current_type == 'punc' and not current_token == '(':
                    raise ValueError(f'"{current_token}" may not follow a +')
                elif current_type == 'punc' and current_token == '(':
                    hints, _, string_only = self._read_expression(lexer)
                    if string_only:
                        pass
                    if argument_mode:
                        log_string += '{}'
                        arguments.append(hints[0])
                    else:
                        arguments.append(hints[0])

                elif current_type == 'var':
                    variable = self._read_var(lexer)
                    if argument_mode:
                        log_string += '{}'
                        arguments.append(variable)
                    else:
                        arguments.append(variable)
            elif current_type == 'punc' and current_token == ',':
                argument_mode = False
                lexer.next()
            elif current_type == 'op' and lexer.is_unary_ops(current_token):
                lexer.next()
            elif current_type == 'var':
                _, expression, _ = self._read_expression(lexer)
                if 'String.format' in expression:
                    expression = expression.replace("String.format(", '')
                    expression = expression[:expression.rindex(')')]
                    tmp = self._parse(expression)
                    return tmp
                    # handle this here:
                if argument_mode:
                    log_string += '{}'
                else:
                    arguments.append(expression)
            elif current_type == 'num':
                dtype, value = self._check_number(current_token)
                if argument_mode:
                    log_string += '{}'
                    arguments.append('{!Integer}' if dtype == 'int' else '{!Float}')
                else:
                    arguments.append('{!Integer}' if dtype == 'int' else '{!Float}')
                lexer.next()
            elif current_type == 'punc' and current_token == '(':
                hints, output, string_only = self._read_expression(lexer)
                if string_only:
                    stream = JavaTokenizer(Stream(output))
                    constructed_token = ""
                    while not stream.eof():
                        if (token := stream.next())[0] == 'str':
                            constructed_token += token[1]
                    log_string += constructed_token
                elif argument_mode:
                    log_string += '{}'
                else:
                    arguments.append(hints[0])
        return log_string, arguments
        # print(f'Original line: {input}\n'
        #       f'Parsed Log-String: {log_string}\n'
        #       f'Parsed Arguments: {arguments}')

    def _read_var(self, lexer: JavaTokenizer):
        initial_type, var_tokens = lexer.peek()
        if initial_type != 'var':
            raise ValueError('Called _read_var on a stream that\'s not pointing to a var')
        stack = []
        current_type, current_token = lexer.next()
        while not lexer.eof():
            next_type, next_token = lexer.peek()
            if current_type == 'var' and (next_type == 'op' and next_token == '.'):
                var_tokens += next_token
                lexer.next()
            elif not stack and (current_type == 'var' and (next_type == 'op' and next_token == '+')):
                break
            elif not stack and (current_type == 'var' and (next_type == 'punc' and next_token == ',')):
                break
            elif next_type == 'punc' and next_token == '(':
                lexer.next()
                stack.append(True)
            elif next_type == 'punc' and next_token == ')':
                lexer.next()
                stack.pop()
            elif current_type != 'var' and next_type == 'var':
                var_tokens += next_token
                lexer.next()
            else:
                var_tokens += next_token
                lexer.next()
        return var_tokens

    def _read_expression(self, lexer: JavaTokenizer):
        value_hint = []
        stack = []
        string_only = True
        original_string = ""  # current_token
        while not lexer.eof():
            next_type, next_token = lexer.peek()
            if next_type == 'str':
                original_string += f'"{next_token}"'
            else:
                original_string += next_token
            if next_type == 'punc' and next_token == '(':
                lexer.next()
                stack.append(True)
            elif next_type == 'punc' and next_token == ')':
                lexer.next()
                stack.pop()
            elif next_type == 'punc' and next_token == ')':
                lexer.next()
                stack.pop()
                string_only = False
            elif not stack and (next_type == 'punc' and next_token == ','):
                break
            elif not stack and (next_type == 'punc' and next_token == ';'):
                break
            elif next_type == 'var':
                value_hint.append(next_token)
                lexer.next()
                string_only = False
            elif next_type == 'num':
                dtype, value = self._check_number(next_token)
                value_hint.append(dtype)
                lexer.next()
                string_only = False
            elif next_type == 'str':
                lexer.next()
            else:
                lexer.next()
                string_only = False
        # print(f'Expression parsed, I would suggest it be: {value_hint}')
        return value_hint, original_string, string_only

    def _check_number(self, number_string: str):
        try:
            value = int(number_string)
            return 'int', value
        except ValueError:
            pass
        try:
            value = float(number_string)
            return 'float', value
        except ValueError:
            pass
