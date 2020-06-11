import json
from typing import Union, List

from .utils import Communicator, get_deepest_dict_value
from .gittypes import GitTree, GitBlob, GitRepo


class GitHubCrawlerCalls:
    api_endpoint = 'https://api.github.com/graphql'

    def __init__(self, auth_token: str, owner: str = None, repository: str = None, use_session: bool = True):
        self.owner = owner
        self.repository = repository
        self._auth_token = auth_token
        self._communicator = Communicator(use_session)

    def get_primary_language(self) -> str:
        if not self.owner or not self.repository:
            raise ValueError('Owner or repository not set')
        query_raw = f"""query {{
                            repository(owner: \"{self.owner}\", name:\"{self.repository}\") {{
                                primaryLanguage {{name}}
                            }}
                        }}"""
        query_skeleton = {'query': query_raw}
        query = json.dumps(query_skeleton)
        header = {'Authorization': f'bearer {self._auth_token}'}
        return get_deepest_dict_value(self._communicator.send_and_receive(header, query).json())

    def get_default_branch(self) -> str:
        if not self.owner or not self.repository:
            raise ValueError('Owner or repository not set')
        query_raw = f"""query {{
                        repository(owner: "{self.owner}", name:"{self.repository}") {{
                            defaultBranchRef {{
                                    name
                            }}
                        }}
                    }}"""
        query_skeleton = {'query': query_raw}
        query = json.dumps(query_skeleton)
        header = {'Authorization': f'bearer {self._auth_token}'}
        return get_deepest_dict_value(self._communicator.send_and_receive(header, query).json())

    def get_root_tree(self) -> GitTree:
        if not self.owner or not self.repository:
            raise ValueError('Owner or repository not set')
        query_raw = f"""query {{
                repository(name: "{self.repository}" owner: "{self.owner}") {{
                    defaultBranchRef {{
                        target {{
                            ... on Commit {{
                                oid
                                tree {{
                                    entries{{
                                        type
                                        name
                                        oid
                                }}
                            }}
                        }}
                    }}
                }}
            }}
        }}"""
        query_skeleton = {'query': query_raw}
        query = json.dumps(query_skeleton)
        header = {'Authorization': f'bearer {self._auth_token}'}
        data = self._communicator.send_and_receive(header, query).json()
        tree_content = data['data']['repository']['defaultBranchRef']['target']['tree']['entries']
        oid = data['data']['repository']['defaultBranchRef']['target']['oid']
        return self._fill_tree(oid, tree_content, name='__root__')

    def get_tree(self, target_tree: Union[str, GitTree]) -> GitTree:
        if not self.owner or not self.repository:
            raise ValueError('Owner or repository not set')
        if type(target_tree) == GitTree:
            oid = target_tree.oid
        else:
            oid = target_tree

        query_raw = f"""query {{
                repository(name: "{self.repository}" owner: "{self.owner}") {{
                    object(oid: "{oid}") {{
                        ... on Tree {{
                                entries {{
                                    type
                                    name
                                    oid
                                }}
                            
                        }}
                    }}
                }}
        }}"""
        query_skeleton = {'query': query_raw}
        query = json.dumps(query_skeleton)
        header = {'Authorization': f'bearer {self._auth_token}'}
        data = self._communicator.send_and_receive(header, query).json()
        tree_content = data['data']['repository']['object']['entries']

        if type(target_tree) == GitTree:
            tmp = self._fill_tree(None, tree_content)
            target_tree.entries = tmp.entries
            return target_tree
        else:
            return self._fill_tree(oid, tree_content)

    def search_for_repos(self, language: str, count: int, cursor: str = None, after=True) -> List[GitRepo]:
        if cursor:
            direction_keyword = 'after' if after else 'before'
            _cursor = direction_keyword + f': "{cursor}"'
        else:
            _cursor = ""

        query_raw = f"""query {{
                search(query: "language:{language}" type: REPOSITORY first: {str(count)} {_cursor}) {{
                    edges {{
                        cursor
                        node {{
                            ... on Repository {{
                                languages (first: 2){{
                                    edges {{
                                      node {{
                                        name
                                      }}
                                    }}
                                  }}
                                nameWithOwner
                                url
                                stargazers {{
                                  totalCount
                                }}
                                isFork
                                diskUsage
                                licenseInfo {{
                                    name
                                    key
                                    url
                                }}
                            }}
                        }}
                    }}
                }}
        }}"""
        query_skeleton = {'query': query_raw}
        query = json.dumps(query_skeleton)
        header = {'Authorization': f'bearer {self._auth_token}'}
        data = self._communicator.send_and_receive(header, query).json()
        data_root = data['data']['search']['edges']
        repositories = []
        for entry in data_root:
            cursor_data = entry['cursor']
            repo_data = entry['node']
            language_data = [x['node']['name'] for x in repo_data['languages']['edges']]
            star_data = repo_data['stargazers']['totalCount']
            if repo_data['licenseInfo']:
                license_name = repo_data['licenseInfo']['name']
                license_key = repo_data['licenseInfo']['key']
            else:
                license_name = ''
                license_key = ''
            name, owner = repo_data['nameWithOwner'].split('/')
            repositories.append(GitRepo(name=name, owner=owner, url=repo_data['url'], stars=star_data,
                                        is_fork=repo_data['isFork'], disk_usage=repo_data['diskUsage'],
                                        license=license_name, license_key=license_key,
                                        languages=language_data, cursor=cursor_data))
        return repositories

    def get_blob(self, target_blob: Union[str, GitBlob]) -> GitBlob:
        if not self.owner or not self.repository:
            raise ValueError('Owner or repository not set')
        if type(target_blob) == GitBlob:
            oid = target_blob.oid
        else:
            oid = target_blob
        query_raw = f"""query {{
                repository(name: "{self.repository}" owner: "{self.owner}") {{
                    object(oid: "{oid}") {{
                        ... on Blob {{
                            isBinary
                            byteSize
                            text
                        }}
                    }}
                }}
            }}"""
        query_skeleton = {'query': query_raw}
        query = json.dumps(query_skeleton)
        header = {'Authorization': f'bearer {self._auth_token}'}
        _data = self._communicator.send_and_receive(header, query).json()
        data = _data['data']['repository']['object']
        if data['isBinary']:
            raise ValueError("Binary file fetched")
        content = data['text']
        size = data['byteSize']
        if type(target_blob) == GitBlob:
            target_blob.content = content
            target_blob.size = size
            return target_blob
        else:
            return GitBlob(name=None, oid=oid, type='blob', size=size, content=content)

    def _fill_tree(self, oid, tree_content, name: str = None) -> GitTree:
        entries = []
        for x in tree_content:
            if x['type'] == 'tree':
                entries.append(GitTree(x['name'], x['oid'], x['type'], entries=None))
            elif x['type'] == 'blob':
                entries.append(GitBlob(x['name'], x['oid'], x['type'], size=None, content=None))
            else:
                raise ValueError(f"Unexpected GitObject type. Expected 'tree' or 'blob' but got '{x['type']}' instead")
        return GitTree(name=name, oid=oid, entries=entries, type='tree')
