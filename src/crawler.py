from typing import List, Union
from random import sample
from pathlib import Path
from git import Repo

from crawlerengine.calls import GitHubCrawlerCalls
from crawlerengine.patterns import LanguageMap
from crawlerengine.heuristicwalk import HeuristicDeepWalk
from crawlerengine.gittypes import GitTree, GitBlob


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

    def fetch_repository(self, destination: Union[str, Path]):
        Repo.clone_from(f'https://github.com/{self.owner}/{self.repository}', str(destination))

    def fetch_primary_language(self):
        self._language = self._caller.get_primary_language()
        self._extensions = LanguageMap[self._language.lower()]

    def _fetch_root_tree(self) -> GitTree:
        return self._caller.get_root_tree()

