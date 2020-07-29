from typing import List

from templatecrawler.logdetector import python, csharp
from templatecrawler.logdetector import java


class LogDetector:

    JAVA = 'java'
    PYTHON = 'python'
    CSHARP = 'csharp'

    _engine_selector = {'java': java.DetectorEngine,
                        'python': python.DetectorEngine,
                        'csharp': csharp.DetectorEngine}

    def __init__(self, language: str):
        self.language = language
        self._engine = self._engine_selector[language]()

    def from_files(self, files: List[str]):
        contains_logging, framework_indicators = [self._engine.process_file(x) for x in files]
        return any(contains_logging), max(framework_indicators, key=framework_indicators.count)

    def from_dependencies(self, dependency_file: str):
        raise NotImplementedError

