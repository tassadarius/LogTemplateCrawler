import re


class DetectorEngine:

    _regex_rules = {
        'logger_import': re.compile(r"import logging"),
        'log_statement': re.compile(r"\.log.*\("),
    }

    def process_file(self, content: str) -> bool:

        result = []

        for key, regex in self._regex_rules.items():
            re_match = re.search(regex, content)
            if re_match:
                print(f'{key} matched')
                result.append(True)
            else:
                result.append(False)
        return any(result)
