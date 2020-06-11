from typing import List

class TokenType:

    def __init__(self, name: str, dtype, keywords: List[str]):
        self.name = name
        self.dtype = dtype
        self.keywords = keywords


integer_token = TokenType('IntegerPlaceholder', int,
                          ['number', 'num', 'integer', 'int', 'index', 'idx', 'size', 'length', 'count', 'capacity',
                           'per', 'offset'])
float_token = TokenType('FloatPlaceholder', float,
                        ['number', 'num', 'float', 'double', 'ratio', 'size', 'per', 'frequency', 'interval'])
path_token = TokenType('PathPlaceholder', str,
                       ['path', 'dir', 'directory', 'location', 'file'])
url_token = TokenType('URLPlaceholder', str, ['address', 'host', 'addr', 'url', 'uri'])
time_token = TokenType('TimePlaceholder', int, ['time', 'seconds', 'date', 'timestamp'])
date_token = TokenType('DatePlaceholder', str, ['time', 'timestamp', 'date', 'today', 'now'])
id_token = TokenType('IDPlaceholder', str, ['id', 'identifier'])

tokens = [integer_token, float_token, path_token, url_token, time_token, date_token, id_token]





