class Stream:

    def __init__(self, string: str):
        self.s = string
        self.pos = 0

    def next(self):
        character = self.s[self.pos]
        self.pos += 1
        return character

    def peek(self):
        return self.s[self.pos]

    def eof(self):
        return self.pos >= len(self.s)

    def croak(self, msg):
        raise ValueError(f'{msg} [offset {self.pos}]')

