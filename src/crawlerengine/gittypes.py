from dataclasses import dataclass
from typing import List, Union


@dataclass
class GitObject:
    name: Union[str, None]                      # Sometimes this may need to be None when property has not been fetched
    oid: str
    type: str


@dataclass
class GitTree(GitObject):
    entries: Union[List[GitObject], None]       # Sometimes this may need to be None when property has not been fetched


@dataclass
class GitBlob(GitObject):
    size: Union[int, None]                      # Sometimes this may need to be None when property has not been fetched
    content: Union[str, None]                   # Sometimes this may need to be None when property has not been fetched

    def __gt__(self, other):
        return self.size > other.size

    def __ge__(self, other):
        return self.size >= other.size

    def __lt__(self, other):
        return self.size < other.size

    def __le__(self, other):
        return self.size <= other.size

    def __eq__(self, other):
        return self.size == other.size


@dataclass
class GitRepo:
    name: str
    owner: str
    url: str
    stars: int
    is_fork: bool
    disk_usage: float
    license: str
    license_key: str
    languages: List[str]
    cursor: str


