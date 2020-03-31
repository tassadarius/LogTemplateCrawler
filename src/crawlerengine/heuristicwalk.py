from typing import Set, Dict, List
from random import sample

from .calls import GitHubCrawlerCalls
from .gittypes import GitTree, GitBlob


_priority_dirs = {'src', 'source'}
_exclude_dirs = {'doc', 'docs', 'examples', 'test', 'tests', 'testing', 'tmp'}


class HeuristicDeepWalk:
    def __init__(self, crawler: GitHubCrawlerCalls, root_directory: GitTree, file_endings: Set[str], repo_name: str):
        self._crawler = crawler
        self._root_directory = root_directory
        self.files = []
        self._file_endings = tuple(file_endings)
        self._repo_name = repo_name
        self._split_depth = None

    def retreive_file_list(self, splits=4, split_depth=2) -> List[GitBlob]:
        self._split_depth = split_depth
        self._walk_first_layer(splits=splits)

        # Download actual content
        _result = []
        for blob in self.files:
            _result.append(self._crawler.get_blob(blob))
        result = [x for x in _result if x.size > 255]             # Filter out very small or empty files
        return sorted(result, reverse=True)

    def _filter_trees(self, tree: GitTree) -> List[GitTree]:
        return [x for x in tree.entries if type(x) == GitTree]

    def _find_priority_trees(self, project_name: str, trees: List[GitTree]) -> List[GitTree]:
        _name = project_name.lower()
        return [x for x in trees if x.name in _priority_dirs or x.name.lower() == _name]

    def _exclude_unimportant_trees(self, tree: List[GitTree]):
        return [x for x in tree if x.name not in _exclude_dirs]

    def _deep_walk(self, tree: GitTree, split: int):
        if self._split_depth < 0:
            split = 1
        else:
            self._split_depth -= 1

        tree = self._crawler.get_tree(target_tree=tree)
        self.files += self._select_files(tree)
        tree_directories = self._filter_trees(tree)
        tree_directories = self._exclude_unimportant_trees(tree_directories)
        next_trees = self._select_random_trees(tree_directories, split)
        for _tree in next_trees:
            self._deep_walk(_tree, split)

    def _walk_first_layer(self, splits: int):
        self.files += self._select_files(self._root_directory)
        tree_directories = self._filter_trees(self._root_directory)
        tree_directories = self._exclude_unimportant_trees(tree_directories)
        priority_dirs = self._find_priority_trees(self._repo_name, tree_directories)
        self._split_depth -= 1

        if priority_dirs:
            for _tree in priority_dirs:
                self._deep_walk(_tree, splits)
        else:
            for _tree in self._select_random_trees(tree_directories, splits):
                self._deep_walk(_tree, splits)

    def _select_random_trees(self, trees: List[GitTree], count: int) -> List[GitTree]:
        if len(trees) == 0:
            return []
        elif 0 > len(trees) <= count:
            return trees
        else:
            return [x for x in sample(trees, count)]

    def _select_files(self, tree: GitTree) -> List[GitBlob]:
        return [x for x in tree.entries if type(x) == GitBlob and x.name.endswith(self._file_endings)]


