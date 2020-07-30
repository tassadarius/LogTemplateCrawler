import re
from typing import Tuple


class DetectorEngine:

    _regex_rules = {
        'log4j_import': (re.compile(r"import org.apache.log4j"), 'log4j'),
        'log4j_statement': (re.compile(f"\.(debug|info|warn|error|fatal)"), 'log4j'),
        'utillogger_import': (re.compile(r"import java.util.logging.Logger"), 'utillogger'),
        'utillogger_statement': (re.compile(r"\.severe|warning|info|config|fine|finer|finest|log"), 'utillogger'),
        'slf4j_import': (re.compile(f"import org.slf4j.Logger;"), 'slf4j'),
    }

    def process_file(self, content: str) -> Tuple[bool, str]:
        result = []
        indicators = []
        for key, (regex, indicator) in self._regex_rules.items():
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
