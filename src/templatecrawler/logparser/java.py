import pandas as pd
import re
from typing import Tuple, List
import logging

from templatecrawler.logparser import filtersettings as fs
from templatecrawler.logparser.strstream import Stream
from templatecrawler.logparser.javatokenizer import JavaTokenizer


class JavaParser:

    log = logging.getLogger(__name__)
    _general_map = {
        'format': 'format',
        'printf': 'format'
    }
    _slf4j_map = {
        'trace': 'format',
        'debug': 'format',
        'info': 'format',
        'warn': 'format',
        'error': 'format'
    }
    _log4j_map = _slf4j_map

    _utillogger_map = {
        'fine': 'simple',
        'finer': 'simple',
        'finest': 'simple',
        'severe': 'simple',
        'warning': 'simple',
    }

    _framework_selector = {
        'slf4j': _slf4j_map,
        'log4j': _log4j_map,
        'utillogger': _utillogger_map
    }

    def __init__(self, framework: str):
        self._framework_map = self._framework_selector[framework]
        self._current_template = None

    def run(self, data):
        print(f'Dataset size before filtering is {len(data)}')

        # Filter useless rows
        for current_filter in fs.filter_rules:
            mask = data.apply(lambda x: True if re.search(current_filter, x) else False)  # True if filter matches
            data = data[~mask]                                                            # Remove all Trues
            # self.df = self.df[~mask]                                                      # Also in parent structure
            print(f'Removed {sum(mask)} entries from dataset. New size is {len(data)}')

        output = {'parsed_template': [], 'arguments': [], 'raw': []}
        for string in data:
            self._current_template = string
            try:
                result, arguments = self._parse_new(string)
                if result and len(result) > 0:
                    output['parsed_template'].append(result)
                    output['arguments'].append(arguments)
                    output['raw'].append(string)
            except ValueError as e:
                print(f'{string}\nParsing error: ', e)
        return pd.DataFrame(output)

    def _parse(self, inp) -> Tuple[str, List[str]]:
        character_stream = Stream(inp)
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
            else:
                print(f'Weird behavio for token {current_token}<{current_type}>')
                lexer.next()
        return log_string, arguments

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

    def _parse_new(self, inp: str):
        character_stream = Stream(inp)

        lexer = JavaTokenizer(character_stream)
        mode, message, variables = self._read_variable(lexer)
        if mode == 'simple':
            return '', []
        elif mode == 'nested':
            return message, self._flatten(variables)
        else:
            self.log.warning(f'General Parsing problem [Error on evaluating first expression] on <{inp}>')

    def _flatten(self, li):
        output = []
        for element in li:
            if isinstance(element, list):
                output += self._flatten(element)
            else:
                output.append(element)
        return output

    def _parse_format(self, lexer: JavaTokenizer, params: List[str]):

        if not params:
            raise ValueError("Trying to parse format without argument. Aborting...")

        param_offset = 0
        param_type = params[param_offset]
        message = ''
        variables = []
        statement_stack = []
        while not lexer.eof():
            token_type, token = lexer.peek()

            # Advance argument
            if token_type == 'punc' and token == ',' and param_type != '...':
                param_offset = self._increase_index(param_offset, len(params))
                param_type = params[param_offset]

            # New expression
            elif token_type == 'punc' and token == '(':
                statement_stack.append(token)
            # Closing expression
            elif token_type == 'punc' and token == ')':
                statement_stack.pop()
                # No expressions left
                if not statement_stack:
                    break

            # String literal
            elif token_type == 'str' and param_type == '...':
                variables.append(token)
            elif token_type == 'str':
                message += self._parse_format_string(token)

            elif token_type == 'num' and param_type == 'str':
                message += str(token)
            elif token_type == 'num' and param_type == '...':
                variables.append(token)

            # Variable
            elif token_type == 'var' or (token_type == 'op' and lexer.is_unary_ops(token)):

                # If it is not, handle as normal variable
                var_type, tokens, arguments = self._read_variable(lexer)
                if var_type == 'simple':
                    variables.append(''.join(tokens))
                    if param_type == 'str':
                        message += '{}'
                if var_type == 'nested':
                    message += tokens
                    variables.append(arguments)

            # Operator '+' on string
            elif param_type == 'str' and token_type == 'op' and token == '+':
                lexer.next()
                token_type, token = lexer.peek()
                if token_type == 'str':
                    message += token
                elif token_type == 'var':
                    tmp_mode, tmp_message, tmp_variables = self._read_variable(lexer)

                    if tmp_mode == 'simple' and tmp_message:
                        message += '{}'
                        variables += tmp_message
                    elif tmp_mode == 'nested':
                        pass



            lexer.next()
        self._parse_format_string(message)
        return message, variables

    def _parse_simple(self, lexer: JavaTokenizer):
        return '', []

    def _parse_printf(self, lexer: JavaTokenizer, params: List[str]):
        if params != ['str', '...'] and params != ['str']:
            raise ValueError(f"Got unexpected params <{params}> for '{lexer.input.s}'")
        return '', []

    def _get_format_expression(self, lexer: JavaTokenizer):
        token_type, token = lexer.peek()
        if token_type != 'var':
            self.log.info(f'Unexpected BOF token: {token_type}  ({token})')

        stack = []
        while not lexer.eof():
            token_type, token = lexer.peek()
            if token_type == 'punc' and token == '(':
                return stack
            stack.append(token)
            lexer.next()

    def _map_function(self, function_name):
        """ There are 3 major styles of functions:
            * printf
            * format
            * simple

        :return:
        """
        function_name = function_name.strip()
        if function_name in self._framework_map.keys():
            return self._framework_map[function_name]
        elif function_name in self._general_map.keys():
            return self._general_map[function_name]
        else:
            return None

    _processing_map = {
        'format': _parse_format,
        'simple': _parse_simple,
        'printf': _parse_printf
    }

    def _read_variable(self, lexer: JavaTokenizer):
        stack = []
        variable_name = []
        previous_was_var = False
        while not lexer.eof():
            token_type, token = lexer.peek()
            if token_type == 'punc' and token == ',' and not stack:
                return 'simple', variable_name, None
            elif token_type == 'op' and token == '+' and not stack:
                return 'simple', variable_name, None
            elif token_type == 'var':
                previous_was_var = True

            # Function Call
            elif token_type == 'punc' and token == '(' and previous_was_var:
                previous_was_var = False
                mapping = self._map_function(variable_name[-1])

                # If it is another formatting call, follow it
                if mapping in self._processing_map.keys():
                    _new_stream = Stream(lexer.input.s[lexer.input.pos - 1:])
                    _new_lexer = JavaTokenizer(_new_stream)
                    args = self._count_arguments(_new_lexer)
                    param_mapping = self._create_params_mapping('unknown', args)
                    # print(f'[NESTED] Argument count for {_new_lexer.input.s} --> {args}')
                    func = self._processing_map[mapping]
                    msg, variables = func(self, lexer, param_mapping)
                    return 'nested', msg, variables
                # Elsewise we handle as normal expression bracket
                else:
                    stack.append('(')

            # Bracket expression
            elif token_type == 'punc' and token == '(':
                previous_was_var = False
                stack.append('(')
            elif token_type == 'punc' and token == ')':
                previous_was_var = False
                # If there's a closing bracket without an opening one it is not ours again and we release
                if not stack:
                    return 'simple', variable_name, None
                # In this case it is our bracket and we pop it
                else:
                    stack.pop()
            else:
                previous_was_var = False
            variable_name.append(token)
            lexer.next()
        raise ValueError('Unexpected EOF')

    def _increase_index(self, old_value, list_size):
        new_value = old_value + 1
        if new_value >= list_size:
            msg = f'Missparsing number of arguments on <{self._current_template}>'
            logging.warning(msg)
            raise ValueError(msg)
        return new_value

    def _count_arguments(self, lexer: JavaTokenizer):
        previous_token_type = None
        previous_token = None
        while not lexer.eof():
            token_type, token = lexer.peek()
            if token_type == 'punc' and token == '(':
                previous_token_type = token_type
                previous_token = token
                lexer.next()
                break
            lexer.next()
        if lexer.eof():
            raise ValueError(f'Does not contain a function call')

        stack = []
        argument_count = 1
        while not lexer.eof():
            token_type, token = lexer.peek()

            if token_type == 'punc' and token == ')' and not stack:
                if previous_token_type and previous_token_type == 'punc' and previous_token and previous_token == '(':
                    return 0
                return argument_count
            elif token_type == 'punc' and token == '(':
                stack.append('(')
            elif token_type == 'punc' and token == ')':
                stack.pop()
            elif token_type == 'punc' and token == ',' and not stack:
                argument_count += 1
            previous_token_type = token_type
            previous_token = token
            lexer.next()
        return argument_count

    def _create_params_mapping(self, name: str, argument_count: int):
        mapping = {
            0: [],
            1: ['str'],
            2: ['str', '...']
        }
        if argument_count in mapping.keys():
            return mapping[argument_count]
        else:
            return mapping[2]

    percentage_re = re.compile('%[0-9+#-.]*[l|hh|ll|j|z|tL]?[diuoxXfFeEgGaAcspn%]')

    def _parse_format_string(self, fstring):
        percentage_matches = re.findall(self.percentage_re, fstring)

        if percentage_matches:
            result = re.sub(self.percentage_re, '{}', fstring)
            return result
        return fstring


    def _follow_percent(self, stream: Stream):
        pass

    def _follow_bracket(self, stream: Stream):
        pass
