from typing import List, Union
from random import sample
from pathlib import Path
from git import Repo, CommandError, GitCommandError, GitCommandNotFound
from filelock import FileLock
import pandas as pd
from dataclasses import asdict
import base64
import shutil
import psycopg2

from templatecrawler.crawlerengine.calls import GitHubCrawlerCalls
from templatecrawler.crawlerengine.patterns import LanguageMap
from templatecrawler.crawlerengine.heuristicwalk import HeuristicDeepWalk
from templatecrawler.crawlerengine.gittypes import GitTree, GitBlob


class GitHubCrawler:
    def __init__(self, auth_token: str, owner: str, repository: str,):
        self.auth_token = auth_token
        self.owner = owner
        self.repository = repository
        self._caller = GitHubCrawlerCalls(auth_token, owner, repository)
        self._language = None
        self._extensions = None

    def fetch_repository_tree(self, delay: int):
        pass

    def fetch_file(self, oid: str):
        pass

    def fetch_tree(self, oid: str):
        pass

    def fetch_heuristically(self, file_count: int) -> List[GitBlob]:
        if not self._language:
            self.fetch_primary_language()

        root_tree = self._fetch_root_tree()

        walker = HeuristicDeepWalk(crawler=self._caller, root_directory=root_tree,
                                   file_endings=self._extensions, repo_name=self.repository)
        files = walker.retreive_file_list(4, split_depth=1)

        # Cut down the list to the size we want. We sample a little bit so it doesn't always take the largest files
        if len(files) > file_count:
            max_pool_samples = min(len(files), 2 * file_count)
            return sample(files[:max_pool_samples], file_count)
        else:
            return files

    @staticmethod
    def _update(op_code, cur_count, max_count=None, message=''):
        if max_count:
            print(f'Downloading repository: {cur_count}/{max_count}\r', end="")

    def fetch_repository(self, destination: Union[str, Path]):
        _destination = Path(destination, self.repository)
        if _destination.exists():
            return _destination
        try:
            Repo.clone_from(f'https://github.com/{self.owner}/{self.repository}', str(_destination), progress=GitHubCrawler._update)
        except (CommandError, GitCommandError, GitCommandNotFound) as e:
            raise ValueError(f'Git command {e.command} failed')
        return str(_destination.absolute())

    def fetch_primary_language(self):
        self._language = self._caller.get_primary_language().lower()
        self._extensions = LanguageMap[self._language]
        return self._language

    def _fetch_root_tree(self) -> GitTree:
        return self._caller.get_root_tree()

    def delete(self, path):
        shutil.rmtree(path)


class GitHubSearcherCSV:
    def __init__(self, csv_file: Union[str, Path], auth_token: str):
        if type(csv_file) == str:
            self._csv_path = Path(csv_file)
        else:
            self._csv_path = csv_file
        if not self._csv_path.exists():
            self._csv_path.touch()
        self._lock = None
        self._with_block = False
        self._old_df = None                                             # type: Union[pd.DataFrame, None]
        self._df = None                                                 # type: Union[pd.DataFrame, None]
        self._caller = GitHubCrawlerCalls(auth_token=auth_token)

    def __enter__(self):
        self._with_block = True
        self._lock = FileLock(self._csv_path)
        if self._csv_path.stat().st_size > 0:
            self._old_df = pd.read_csv(self._csv_path)
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self._df is not None and not self._df.empty:
            self._df.to_csv(self._csv_path, mode='a')
        self._lock.release()
        self._with_block = False

    def repositories(self, language: str, count: int = 100):
        if not self._with_block:
            raise RuntimeError(f'{type(self).__name__} should only be used within a with statement')
        if self._old_df and not self._old_df.empty:
            cursor = self._old_df.tail(1).cursor
        else:
            cursor = None
        data = self._caller.search_for_repos(language, count, cursor)
        # Convert the GitRepo objects to a List of Dictionaries which can be easily converted into DataFrames
        self._df = pd.DataFrame([asdict(x) for x in data])

    def _filter_valid(self):
        # Filter all which are already in the list
        
        # Filter all which have less than 1 MB of data
        # Filter all forks
        # (Optional) Filter all with less than 1 star
        # (Optional) Licenses may be problematic
        pass


class GitHubSearcher:
    def __init__(self, auth_token: str):
        self._caller = GitHubCrawlerCalls(auth_token=auth_token)
        self._df = None

    def repositories(self, language: str, count=100, cursor: int = None):
        if cursor:
            target = f'cursor:{cursor}'
            cursor_as_bytes = base64.b64encode(bytes(target, encoding='utf-8'))
            cursor = str(cursor_as_bytes, encoding='utf-8')

        data = self._caller.search_for_repos(language, count, cursor)
        self._df = pd.DataFrame([asdict(x) for x in data])
        return self._df

