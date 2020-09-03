import re
from typing import Tuple
from itertools import chain


class DetectorEngine:

    _statement_rules = {
        'log4j_statement': (re.compile(r"\.(debug|info|warn|error|fatal)"), 'log4j'),
        'utillogger_statement': (re.compile(r"\.severe|warning|info|config|fine|finer|finest|log"), 'utillogger'),
        'slf4j_statement': (re.compile(r"\.(debug|info|warn|error|fatal)"), 'slf4j'),

    }

    _import_rules = {
        'log4j_import': (re.compile(r"import.+log4j"), 'log4j'),
        'utillogger_import': (re.compile(r"import.+util\.logging"), 'utillogger'),
        'slf4j_import': (re.compile(f"import.+slf4j"), 'slf4j'),
    }

    def process_file(self, content: str) -> Tuple[bool, str]:
        result = []
        indicators = []
        for key, (regex, indicator) in chain(self._import_rules.items(), self._statement_rules.items()):
            re_match = re.search(regex, content)
            if re_match:
                result.append(True)
                indicators.append(indicator)
            else:
                result.append(False)
                indicators.append(None)
        framework_indicators = list(filter(None, indicators))
        if framework_indicators:
            framework_indicators = max(framework_indicators, key=framework_indicators.count)
        else:
            framework_indicators = None
        return any(result), framework_indicators

    def detect_framework(self, content: str):
        indicators = []
        for key, (regex, indicator) in self._import_rules.items():
            re_match = re.search(regex, content)
            if re_match:
                indicators.append(indicator)
        return max(indicators, key=indicators.count) if indicators else None



