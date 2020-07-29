import re
from typing import Tuple, List


class DetectorEngine:

    _regex_rules = {
        'log4j_import': (re.compile(r"import org.apache.log4j"), 'log4j'),
        'log4j_statement': (re.compile(f"\.(debug|info|warn|error|fatal)"), 'log4j'),
        'utillogger_import': (re.compile(r"import java.util.logging.Logger"), 'utillogger'),
        'utillogger_statement': (re.compile(r"\.severe|warning|info|config|fine|finer|finest|log"), 'utilloger'),
        'slf4j_import': (re.compile(f"import org.slf4j.Logger;"), 'slf4j'),
    }

    def process_file(self, content: str) -> Tuple[bool, List[str]]:
        result = []
        indicators = []
        for key, (regex, indicator) in self._regex_rules.items():
            re_match = re.search(regex, content)
            if re_match:
                result.append(True)
                indicators.append(indicator)
            else:
                result.append(False)
        return any(result), indicators
