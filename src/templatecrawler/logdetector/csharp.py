import re


class DetectorEngine:

    _regex_rules = {
        '.net_init': re.compile(r"ILogger|LoggerFactory"),
        '.net_statement': re.compile(r"\.(Verbose|Debug|Information|Warning|Error|Fatal)"),
        'serilog_statement': re.compile(r"Log\.(Trace|Debug|Information|Warning|Error|Critical)"),
    }

    def process_file(self, content: str) -> bool:
        raise NotImplementedError
        result = []

        for key, regex in self._regex_rules.items():
            re_match = re.search(regex, content)
            if re_match:
                print(f'{key} matched')
                result.append(True)
            else:
                result.append(False)
        return any(result)

    def process_file(self, content: str) -> bool:
        return False
