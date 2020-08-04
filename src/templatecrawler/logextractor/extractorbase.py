from abc import ABC, abstractmethod
from typing import Union
from pathlib import Path


class ExtractorBase(ABC):

    def __init__(self, repo_path: Union[str, Path]):
        self._path = repo_path if isinstance(repo_path, Path) else Path(repo_path)
        self._df = None
        self._log_statements = None
        self._log_statement_files = None
        self._stream = {}

    @abstractmethod
    def extract_events(self):
        ...

    @abstractmethod
    def get_event_count(self):
        ...

    @abstractmethod
    def save(self, path: Union[str, Path], repo_name: str, repo_url: str):
        ...

