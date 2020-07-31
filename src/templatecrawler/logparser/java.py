import pandas as pd
import re
from typing import Tuple, List

from templatecrawler.logparser import filtersettings as fs
from templatecrawler.logparser.strstream import Stream
from templatecrawler.logparser.javatokenizer import JavaTokenizer


class JavaParser:


    _general_map = {
        'format': 'format',
        'printf': 'printf'
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
        self.framework_map = self._framework_selector[framework]

    def run(self, data):
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
                result, arguments = self._parse_new(string)
                output['template'].append(result)
                output['arguments'].append(arguments)
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
            else:
                print(f'Weird behavio for token {current_token}<{current_type}>')
                lexer.next()
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

    def _parse_new(self, inp: str):
        character_stream = Stream(inp)
        lexer = JavaTokenizer(character_stream)

#         initial_expression = self._get_format_expression(lexer)
#         mapping = ''
#         try:
#             mapping = self._map_function(initial_expression[-1])
#             print(f"{''.join(initial_expression)} detected as {mapping}")
#         except ValueError as e:
#             print(f"{''.join(initial_expression)} could not be mapped {str(e)}")
#             return 'a', 'b'
#
#        message, variables = self.processing_map[mapping](lexer)
        mode, message, variables = self._read_variable(lexer)
        if mode == 'simple':
            return '', []
        elif mode == 'nested':
            return message, variables
        else:
            print('Don\'t know what to do')

    def _parse_format(self, lexer: JavaTokenizer):
        params = ['str', '...']
        param_offset = 0
        param_type = params[param_offset]
        message = ''
        variables = []
        statement_stack = []
        while not lexer.eof():
            token_type, token = lexer.peek()

            # Advance argument
            if token_type == 'punc' and token == ',':
                param_offset += 1
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
                message += token

            elif token_type == 'num' and param_type == 'str':
                message += str(token)

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

            lexer.next()
        return message, variables

            # Operator



    def _parse_simple(self, lexer: JavaTokenizer):
        return '', []

    def _parse_printf(self, lexer: JavaTokenizer):
        return '', []

    def _get_format_expression(self, lexer: JavaTokenizer):
        token_type, token = lexer.peek()
        if token_type != 'var':
            print(f'Unexpected BOF token: {token_type}  ({token})')

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
        if function_name in self.framework_map.keys():
            return self.framework_map[function_name]
        elif function_name in self._general_map.keys():
            return self._general_map[function_name]
        else:
            return None

    _processing_map = {
        'format': _parse_format,
        'simple': _parse_simple,
        'printf': _parse_printf
    }

    def _read_variable(self, lexer):
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
                    func = self._processing_map[mapping]
                    msg, variables = func(self, lexer)
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

