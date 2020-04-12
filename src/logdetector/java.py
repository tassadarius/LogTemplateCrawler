import re


class DetectorEngine:

    _regex_rules = {
        'log4j_import': re.compile(r"import org.apache.log4j"),
        'log4j_statement': re.compile(f"\.(debug|info|warn|error|fatal)"),
        'utillogger_import': re.compile(r"import java.util.logging.Logger"),
        'utillogger_statement': re.compile(r"\.severe|warning|info|config|fine|finer|finest|log"),
        'slf4j_import': re.compile(f"import org.slf4j.Logger;"),
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
