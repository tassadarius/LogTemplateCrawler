import psycopg2
from psycopg2.extras import execute_values
import keyring
import logging
import pandas as pd
import base64
import string
import random
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.hooks.postgres_hook import PostgresHook
from airflow.models.taskinstance import TaskInstance
from templatecrawler.crawler import GitHubSearcher, GitHubCrawler
from templatecrawler.detector import LogDetector
from templatecrawler.extractor import LogExtractor
from templatecrawler.parser import LogParser
from templatecrawler.templatefilter import find_valid

import templatecrawler.formalizer as formalizer
import templatecrawler.tokentypes as ttokens

log = logging.getLogger(__name__)


class TestDatabaseOperator(BaseOperator):

    @apply_defaults
    def __init__(self, postgres_conn_id: str, *args, **kwargs):
        super(TestDatabaseOperator, self).__init__(*args, **kwargs)
        self._conn_id = postgres_conn_id

    def execute(self, context):
        pg_hook = PostgresHook(postgres_conn_id=self._conn_id)
        df = pg_hook.get_pandas_df(sql='SELECT * FROM templates')
        task_instance = context['task_instance']            # type: TaskInstance
        task_instance.xcom_push('database_df', df)


class ConsumeDatabaseOperator(BaseOperator):

    @apply_defaults
    def __init__(self, *args, **kwargs):
        super(ConsumeDatabaseOperator, self).__init__(*args, **kwargs)

    def execute(self, context):
        task_instance = context['task_instance']            # type: TaskInstance
        df = task_instance.xcom_pull(key='database_df')
        log.info(f'Received a {type(df)} with {df.shape[0]} entries')


class SearchRepoOperator(BaseOperator):

    @apply_defaults
    def __init__(self,  postgres_conn_id: str, target_language: str = 'random', start_over: bool = False,
                 *args, **kwargs):
        super(SearchRepoOperator, self).__init__(*args, **kwargs)
        self._conn_id = postgres_conn_id
        if target_language == 'random':
            self.target_language = random.choice(['Java', 'C'])
        else:
            self.target_language = target_language
        self.start_over = start_over

    def execute(self, context):
        task_instance = context['task_instance']            # type: TaskInstance
        searcher = GitHubSearcher(auth_token=keyring.get_password('github-token', 'tassadarius'))
        pg_hook = PostgresHook(postgres_conn_id=self._conn_id)

        if self.start_over:
            cursor = None
        else:
            cursor = self._fetch_highest_cursor(pg_hook)

        result = searcher.repositories(self.target_language, count=100, cursor=cursor)
        if len(result) == 0:
            cursor = None
            self._clear_cursor(pg_hook)
            result = searcher.repositories(self.target_language, count=100, cursor=cursor)
        result['cursor'] = self._parse_cursor(result['cursor'])
        task_instance.xcom_push(key='raw_search', value=result)

    def _fetch_highest_cursor(self, pg_hook: PostgresHook):
        latest_cursor, = pg_hook.get_first(f'SELECT cursor FROM cursor ORDER BY date DESC LIMIT 1', True) or (0,)
        return None if latest_cursor == 0 else latest_cursor

    def _clear_cursor(self, pg_hook: PostgresHook):
        pg_hook.run(f'DELETE FROM cursor', True)

    def _parse_cursor(self, cursor_series: pd.Series):
        cur_data = cursor_series.apply(base64.b64decode)
        cur_str = cur_data.apply(str)
        cur_str = cur_str.apply(lambda x: x.split(':')[1])
        cur_str = cur_str.apply(lambda x: x.translate(str.maketrans('', '', string.punctuation)))
        return cur_str.apply(int)


class FilterSearchOperator(BaseOperator):

    @apply_defaults
    def __init__(self, postgres_conn_id: str, *args, **kwargs):
        super(FilterSearchOperator, self).__init__(*args, **kwargs)
        self._conn_id = postgres_conn_id

    def execute(self, context):
        task_instance = context['task_instance']                    # type: TaskInstance
        search_data = task_instance.xcom_pull(key='raw_search')     # type: pd.DataFrame

        if search_data is None or len(search_data) == 0:
            return

        cursor = search_data['cursor'].max()
        accepted = search_data.loc[search_data['disk_usage'] >= 512000]   # Greater than 500 KiB
        rejected = search_data.loc[search_data['disk_usage'] < 512000]   # Smaller than 500 KiB
        pg_hook = PostgresHook(postgres_conn_id=self._conn_id)
        self._write_to_database(accepted, pg_hook, table='repositories')
        self._write_to_database(rejected, pg_hook, table='discarded_repositories')
        self._write_cursor(cursor, pg_hook)

    def _write_to_database(self, df: pd.DataFrame, pg_hook: PostgresHook, table: str) -> None:
        conn = pg_hook.get_conn()
        cur = conn.cursor()

        columns = ['name', 'owner', 'url', 'stars', 'is_fork', 'disk_usage', 'license', 'cursor', 'languages']
        assert all([x in df.columns for x in columns])

        df = df[columns]        # filter to the relevant columns
        data_as_list = df.to_records(index=False).tolist()

        base_query = cur.mogrify(f"""INSERT INTO {table} ({','.join(columns)}) VALUES %s ON CONFLICT DO NOTHING""")
        execute_values(cur, base_query, data_as_list, template=None, page_size=100)
        conn.commit()
        cur.close()

    def _write_cursor(self, cursor: int, pg_hook: PostgresHook):
        _cursor = int(cursor)
        pg_hook.run("""INSERT INTO cursor (cursor) VALUES (%s)""", autocommit=True, parameters=[_cursor])


class FetchFilesOperator(BaseOperator):

    @apply_defaults
    def __init__(self, *args, **kwargs):
        super(FetchFilesOperator, self).__init__(*args, **kwargs)

    def execute(self, context):
        task_instance = context['task_instance']                # type: TaskInstance
        repositories = task_instance.xcom_pull(key='repositories')
        without_files = list()
        with_files = dict()
        for _, repo in repositories.iterrows():
            crawler = GitHubCrawler(auth_token=keyring.get_password('github-token', 'tassadarius'),
                                    owner=repo['owner'], repository=repo['name'])
            files = crawler.fetch_heuristically(30)
            primary_language = crawler.fetch_primary_language()
            tmp_language = primary_language
            if 'java' in tmp_language or 'Java' in tmp_language:
                repositories['languages'] = 'java'
            if tmp_language == 'c':
                repositories['languages'] = 'c'
            if files:
                with_files[repo.repo_id] = files
            else:
                without_files.append(repo.repo_id)
        task_instance.xcom_push(key='repo_with_files', value=with_files)
        task_instance.xcom_push(key='repo_without_files', value=with_files)
        task_instance.xcom_push(key='repositories', value=repositories)


class DetectLoggingFromFilesOperator(BaseOperator):

    @apply_defaults
    def __init__(self, *args, **kwargs):
        super(DetectLoggingFromFilesOperator, self).__init__(*args, **kwargs)

    def execute(self, context):
        task_instance = context['task_instance']                # type: TaskInstance
        repositories = task_instance.xcom_pull(key='repositories')  # type: pd.DataFrame
        repositories.set_index('repo_id', inplace=True)
        files = task_instance.xcom_pull(key='repo_with_files')

        contains_logging = []
        indicators = []
        for repo, git_objects in files.items():
            language = repositories.loc[repo]['languages']
            detector = LogDetector(language=str(language))
            tmp_files = [git.content for git in git_objects]
            tmp_bool, tmp_indicator = detector.from_files(tmp_files)
            if tmp_bool and tmp_indicator:
                contains_logging.append(tmp_bool)
                indicators.append(tmp_indicator)
                log.info(f'{repositories.loc[repo].owner}/{repositories.loc[repo].name:<25} logging: {tmp_bool}')
            elif tmp_bool and not tmp_indicator:
                log.warning(f'Could find logging but no indicator for {repositories.loc[repo].owner}/{repositories.loc[repo].name:}')
        repositories = repositories.loc[files.keys()]
        repositories['contains_logging'] = contains_logging
        repositories['framework'] = indicators
        task_instance.xcom_push(key='logging_check_from_files', value=repositories)


class DetectLoggingWithoutFilesOperator(BaseOperator):

    @apply_defaults
    def __init__(self, *args, **kwargs):
        super(DetectLoggingWithoutFilesOperator, self).__init__(*args, **kwargs)

    def execute(self, context):
        task_instance = context['task_instance']                # type: TaskInstance
        repositories = task_instance.xcom_pull(key='repositories')  # type: pd.DataFrame
        repositories.set_index('repo_id', inplace=True)
        repo_ids = task_instance.xcom_pull(key='repo_without_files')
        repositories = repositories.loc[repo_ids]

        # Probably yes contains logging or probably no logging
        mask = (repositories['stars'] > 200) & (repositories['disk_usage'] > 256000)
        repositories['contains_logging'] = mask
        task_instance.xcom_push(key='logging_check_without_files', value=repositories)


class CloneAndExtractOperator(BaseOperator):

    @apply_defaults
    def __init__(self, *args, **kwargs):
        super(CloneAndExtractOperator, self).__init__(*args, **kwargs)

    def execute(self, context):
        task_instance = context['task_instance']  # type: TaskInstance
        repositories = task_instance.xcom_pull(key='target_repositories')  # type: pd.DataFrame
        repositories.set_index('repo_id', inplace=True)

        if len(repositories) == 0:
            raise ValueError('Pulled data "target_repositories" is empty')

        extracted = {}
        for idx, repo in repositories.iterrows():
            crawler = GitHubCrawler(auth_token=None, owner=repo['owner'], repository=repo['name'])
            try:
                repo_destination = crawler.fetch_repository('.')
            except ValueError as e:
                logging.warning(e.args)
                continue
            language = repo['languages']
            if type(language) == list:
                if 'java' in language or 'Java' in language:
                    language = 'java'
                elif 'c' in language:
                    language = 'c'
                else:
                    logging.info(f'Language was neither c nor java, only contained {language} ({repo["url"]})')
                    continue

            if len(repo.framework) > 0:
                extractor = LogExtractor(language=language, framework=repo.framework, repository=repo['name'])
                raw_events = extractor.extract()
                extracted[idx] = raw_events

            crawler.delete(path=repo_destination)

        task_instance.xcom_push(key='raw_events', value=extracted)
        task_instance.xcom_push(key='target_repositories', value=repositories)


class ParseOperator(BaseOperator):

    @apply_defaults
    def __init__(self, *args, **kwargs):
        super(ParseOperator, self).__init__(*args, **kwargs)

    def execute(self, context):
        task_instance = context['task_instance']  # type: TaskInstance
        repositories = task_instance.xcom_pull(key='target_repositories')  # type: pd.DataFrame
        data = task_instance.xcom_pull(key='raw_events')

        parsed = None
        for repo_id, extracted_lines in data.items():
            language = repositories.loc[repo_id]['languages']
            if type(language) == list:
                if 'java' in language or 'Java' in language:
                    language = 'java'
                elif 'c' in language:
                    language = 'c'
                else:
                    logging.info(f'Language was neither c nor java, only contained {language} ({repositories.loc[repo_id]["url"]})')
                    continue
            parser = LogParser(language=language)
            result = parser.run(extracted_lines['raw'], framework=repositories.loc[repo_id]['framework'])
            result['repo_id'] = repo_id
            if len(result) == 0:
                continue
            if parsed is None:
                parsed = result
            else:
                parsed = parsed.merge(result, how='outer')
            log.info(f"Parsed {len(result)} events from {repositories.loc[repo_id]['owner']}/{repositories.loc[repo_id]['owner']}")
        task_instance.xcom_push(key='parsed', value=parsed)


class FilterTemplatesOperator(BaseOperator):

    @apply_defaults
    def __init__(self, *args, **kwargs):
        super(FilterTemplatesOperator, self).__init__(*args, **kwargs)

    def execute(self, context):
        task_instance = context['task_instance']  # type: TaskInstance
        data = task_instance.xcom_pull(key='parsed')  # type: pd.DataFrame
        validity_mask = find_valid(data['template'])
        data = data.loc[validity_mask]
        task_instance.xcom_push(key='parsed', value=data)


class FormalizeOperator(BaseOperator):

    @apply_defaults
    def __init__(self, *args, **kwargs):
        super(FormalizeOperator, self).__init__(*args, **kwargs)

    def execute(self, context):
        task_instance = context['task_instance']  # type: TaskInstance
        data = task_instance.xcom_pull(key='parsed')  # type: pd.DataFrame
        output = formalizer.formalize(data, possible_types=ttokens.tokens)
        data = data.loc[output.keys()]
        data['template'] = output.values()
        task_instance.xcom_push(key='formalized', value=data)
