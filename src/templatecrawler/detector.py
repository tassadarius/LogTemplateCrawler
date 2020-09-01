from typing import List

from templatecrawler.logdetector import python, csharp
from templatecrawler.logdetector import java


class LogDetector:

    JAVA = 'java'
    PYTHON = 'python'
    CSHARP = 'csharp'

    _engine_selector = {'java': java.DetectorEngine,
                        'c': java.DetectorEngine,
                        'python': python.DetectorEngine,
                        'csharp': csharp.DetectorEngine}

    def __init__(self, language: str):
        self.language = language
        self._engine = self._engine_selector[language]()

    def from_files(self, files: List[str]):
        result = [self._engine.process_file(x) for x in files]
        contains_logging, framework_indicators = zip(*result)   # Split a List of tuples into two lists
        framework_indicators = list(filter(None, framework_indicators))
        if not framework_indicators:
            return any(contains_logging), []
        return any(contains_logging), max(framework_indicators, key=framework_indicators.count)

    def from_dependencies(self, dependency_file: str):
        raise NotImplementedError

