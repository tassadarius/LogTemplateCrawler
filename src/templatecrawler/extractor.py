from typing import List

from templatecrawler.logextractor.java.log4j import log4jExtractor
from templatecrawler.logextractor.java.slf4j import slf4jExtractor
from templatecrawler.logextractor.java.utillogger import utilloggerExtractor


class LogExtractor:

    JAVA = 'java'
    PYTHON = 'python'
    CSHARP = 'csharp',

    _java_framework_selector = {'log4j': log4jExtractor,
                                'slf4j': slf4jExtractor,
                                'util': utilloggerExtractor,
                                'utillogger': utilloggerExtractor}
    _engine_selector = {'java': _java_framework_selector,
                        'python': NotImplementedError,
                        'csharp': NotImplementedError}

    def __init__(self, language: str, framework: str, repository: str):
        self.language = language
        self._engine = self._engine_selector[language][framework](repository)

    def extract(self):
        return self._engine.extract_events()

