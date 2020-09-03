from datetime import datetime
import pandas as pd
import logging
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.postgres_hook import PostgresHook
from psycopg2.extras import execute_values
from typing import List
from templatecrawler.detector import LogDetector
from templatecrawler.crawler import GitHubCrawler
from templatecrawler.extractor import LogExtractor
from templatecrawler.parser import LogParser
from templatecrawler.templatefilter import find_valid
from templatecrawler.formalizer import formalize
from templatecrawler.tokentypes import tokens
import keyring
from airflow.exceptions import AirflowSkipException


default_args = {
    'postgres_conn_id': 'templates'
}

log = logging.getLogger(__name__)


# Also takes nested empty lists into account
# Taken from https://stackoverflow.com/a/1605679/7307284
def is_list_empty(li: List):
    if isinstance(li, list):  # Is a list
        return all(map(is_list_empty, li))
    return False                  # Not a list


def _release_lock(pg_hook: PostgresHook, repo_id: str):
    db_cursor = pg_hook.get_cursor()
    db_conn = pg_hook.get_conn()
    db_cursor.execute("""UPDATE repositories SET locked = FALSE WHERE repo_id = %s""", [repo_id])
    db_conn.commit()


def _finish_hook(pg_hook: PostgresHook, success: bool, repo_id):
    db_cursor = pg_hook.get_cursor()
    db_conn = pg_hook.get_conn()
    if success:
        db_cursor.execute("""UPDATE repositories SET processed = TRUE, successfully_processed = TRUE 
                          WHERE repo_id = %s""", [repo_id])
    else:
        db_cursor.execute("""UPDATE repositories SET processed = TRUE, successfully_processed = FALSE
                          WHERE repo_id = %s""", [repo_id])
    db_conn.commit()
    _release_lock(pg_hook, repo_id)


def _load_from_database(**context):
    params = context['params']
    postgres_conn_id = params['postgres_conn_id']
    pg_hook = PostgresHook(postgres_conn_id=postgres_conn_id)
    conn = pg_hook.get_conn()
    cur = conn.cursor()

    raw_query = f"""SELECT * from repositories WHERE processed = %s AND locked = %s  LIMIT 1"""
    query = cur.mogrify(raw_query)
    repo_df = pg_hook.get_pandas_df(query, parameters=[False, False])

    if repo_df is None or len(repo_df) != 1:
        raise ValueError("Could not load a valid repository from the database")

    repo = repo_df.iloc[0, :]
    repo['repo_id'] = int(repo['repo_id'])

    log.info(f'Loaded repository {repo["url"]} with ID {repo["repo_id"]}. (stars={repo["stars"]},'
             f'size={repo["disk_usage"]}')

    cur = conn.cursor()
    cur.execute("""UPDATE repositories SET locked = TRUE WHERE repo_id = %s""", [repo['repo_id']])
    conn.commit()
    log.info(f'Aquired lock for repository {repo["url"]} with ID {repo["repo_id"]}')
    task_instance = context['task_instance']
    task_instance.xcom_push('target_repository', repo)
    return True


def _detect(**context):
    task_instance = context['task_instance']
    repo = task_instance.xcom_pull(key='target_repository')
    contains_logging = False
    may_qualify_logging = False

    params = context['params']
    postgres_conn_id = params['postgres_conn_id']
    pg_hook = PostgresHook(postgres_conn_id=postgres_conn_id)
    conn = pg_hook.get_conn()
    cur = conn.cursor()

    crawler = GitHubCrawler(auth_token=keyring.get_password('github-token', 'tassadarius'),
                            owner=repo['owner'], repository=repo['name'])
    git_objects = crawler.fetch_heuristically(40)
    primary_language = crawler.fetch_primary_language()
    tmp_language = primary_language

    # Set the main language in the languages column
    if 'java' in tmp_language or 'Java' in tmp_language:
        repo['main_language'] = 'java'
    if tmp_language == 'c':
        repo['main_language'] = 'c'

    cur.execute("""UPDATE repositories SET main_language = %s WHERE repo_id = %s""",
                [repo['main_language'], repo['repo_id']])
    conn.commit()
    cur.close()

    # Check the popularity and size of the repository
    if repo['stars'] > 200 and repo['disk_usage'] > 256000:
        may_qualify_logging = True

    # It is possible we don't receive any files, if we do we check them.
    if git_objects:

        detector = LogDetector(language=primary_language)
        tmp_files = [git.content for git in git_objects]
        contains_logging, framework_indicator = detector.from_files(tmp_files)

        if framework_indicator:
            repo['framework'] = framework_indicator

    num_files = 'None' if not git_objects else len(git_objects)
    log.info(f"Ran detection on {repo['owner']}/{repo['name']}: Qualifies for logging {may_qualify_logging};"
             f"Fetched {num_files} files. Contain logging {contains_logging}")

    final_flag = contains_logging or may_qualify_logging
    cur = conn.cursor()
    cur.execute("""UPDATE repositories SET contains_logging = %s WHERE repo_id = %s""", [final_flag, repo['repo_id']])
    conn.commit()
    cur.close()

    if not final_flag:
        _finish_hook(pg_hook, success=False, repo_id=repo['repo_id'])

    task_instance.xcom_push(key='target_repository', value=repo)


def _clone(**context):
    task_instance = context['task_instance']
    repo = task_instance.xcom_pull(key='target_repository')  # type: pd.DataFrame

    log.info(f'Starting to clone epository {repo["url"]} with ID {repo["repo_id"]}')
    crawler = GitHubCrawler(auth_token=None, owner=repo['owner'], repository=repo['name'])
    repo_destination = crawler.fetch_repository('.')
    log.info(f'Finished cloning to {repo_destination}')
    task_instance.xcom_push(key='repo_path', value=repo_destination)


def _determine_framework(**context):
    task_instance = context['task_instance']
    repo = task_instance.xcom_pull(key='target_repository')  # type: pd.DataFrame
    repo_path = task_instance.xcom_pull(key='repo_path')  # type: pd.DataFrame

    params = context['params']
    postgres_conn_id = params['postgres_conn_id']
    pg_hook = PostgresHook(postgres_conn_id=postgres_conn_id)
    conn = pg_hook.get_conn()
    cur = conn.cursor()

    crawler = GitHubCrawler(auth_token=None, owner=None, repository=None)
    try:
        files = crawler.fetch_files(path=repo_path, language=repo['main_language'])
        detector = LogDetector(language=repo['main_language'])
        framework = detector.framework(files=files)
    except Exception as e:
        log.error(f'Determining framework failed. Raised exception {e.__class__.__name__}')
        _finish_hook(pg_hook, success=False, repo_id=repo['repo_id'])
        crawler.delete(repo_path)
        raise e

    repo['framework'] = framework
    log.info(f'Determined framework for repository {repo["url"]} with ID {repo["repo_id"]} as <{framework}>')
    task_instance.xcom_push(key='target_repository', value=repo)
    cur.execute("""UPDATE repositories SET framework = %s WHERE repo_id = %s""", [repo['framework'], repo['repo_id']])
    conn.commit()
    cur.close()


def _extract(**context):
    task_instance = context['task_instance']
    repo = task_instance.xcom_pull(key='target_repository')  # type: pd.DataFrame
    repo_path = task_instance.xcom_pull(key='repo_path')

    params = context['params']
    postgres_conn_id = params['postgres_conn_id']
    pg_hook = PostgresHook(postgres_conn_id=postgres_conn_id)

    extractor = LogExtractor(language=repo['main_language'], framework=repo['framework'], repository=repo_path)
    source_lines = extractor.extract()

    # Uncomment this to delete the repository
    # crawler = GitHubCrawler(auth_token=None, owner=None, repository=None)
    #  crawler.delete(repo_path)

    if source_lines is None or len(source_lines) <= 0:
        _finish_hook(pg_hook, success=False, repo_id=repo['repo_id'])
        raise AirflowSkipException()

    log.info(f'Extracting for {repo["url"]} with ID {repo["repo_id"]}'
             f'(main_language: {repo["main_language"]}, framework: {repo["framework"]}) got {len(source_lines)} events')

    task_instance.xcom_push(key='source_lines', value=source_lines)
    return True


def _parse(**context):
    task_instance = context['task_instance']
    repo = task_instance.xcom_pull(key='target_repository')  # type: pd.DataFrame
    source_lines = task_instance.xcom_pull(key='source_lines')

    params = context['params']
    postgres_conn_id = params['postgres_conn_id']
    pg_hook = PostgresHook(postgres_conn_id=postgres_conn_id)

    log.info(f'Parsing raw input {repo["url"]} with ID {repo["repo_id"]}. {len(source_lines)} raw events given.')
    parser = LogParser(language=repo['main_language'])
    parsed_result = parser.run(raw_input=source_lines['raw'], framework=repo['framework'])

    if parsed_result is None or len(parsed_result) <= 0:
        _finish_hook(pg_hook, success=False, repo_id=repo['repo_id'])
        raise AirflowSkipException()
    log.info(f'Parsing result for {repo["url"]} with ID {repo["repo_id"]} are {len(parsed_result)} templates')

    validity_mask = find_valid(parsed_result['parsed_template'])
    parsed_result = parsed_result.loc[validity_mask]

    if parsed_result is None or len(parsed_result) <= 0:
        _finish_hook(pg_hook, success=False, repo_id=repo['repo_id'])
        raise AirflowSkipException()
    log.info(f'Filtering result for {repo["url"]} with ID {repo["repo_id"]} are {len(parsed_result)} templates')

    task_instance.xcom_push(key='raw_templates', value=parsed_result)
    return True


def _formalize(**context):
    task_instance = context['task_instance']
    repo = task_instance.xcom_pull(key='target_repository')  # type: pd.DataFrame
    raw_templates = task_instance.xcom_pull(key='raw_templates')

    params = context['params']
    postgres_conn_id = params['postgres_conn_id']
    pg_hook = PostgresHook(postgres_conn_id=postgres_conn_id)

    log.info(f'formalizing raw input {repo["url"]} with ID {repo["repo_id"]}. {len(raw_templates)} templates given.')
    formalized_output = formalize(data=raw_templates, possible_types=tokens)

    if formalized_output is None or len(formalized_output) <= 0:
        _finish_hook(pg_hook, success=False, repo_id=repo['repo_id'])
        raise AirflowSkipException()

    log.info(f'Finished formalizing for {repo["url"]} with ID {repo["repo_id"]}.'
             f'{len(formalized_output)} final templates')

    # Some elements get filtered out. The return value is a dict of index and result.
    # First we kick out the indices which are not in the result.
    processed_templates = raw_templates.loc[formalized_output.keys()]
    # Then we map the existing results to the correct index, in case they are reordered
    processed_templates['template'] = processed_templates.index.map(formalized_output)
    task_instance.xcom_push(key='templates', value=processed_templates)
    return True


def _update_database(**context):
    task_instance = context['task_instance']
    data = task_instance.xcom_pull(key='templates')
    repo = task_instance.xcom_pull(key='target_repository')  # type: pd.DataFrame

    params = context['params']
    postgres_conn_id = params['postgres_conn_id']
    pg_hook = PostgresHook(postgres_conn_id=postgres_conn_id)

    data['crawl_date'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    data['repo_id'] = repo['repo_id']
    columns = ['template', 'arguments', 'raw', 'repo_id', 'parsed_template', 'crawl_date']
    assert all([x in data.columns for x in columns])

    df = data[columns]  # filter to the relevant columns

    # Here we have to split the query into ones with arguments and one without arguments
    emptiness_mask = df['arguments'].apply(is_list_empty)
    df_args = df.loc[~emptiness_mask]
    df_no_args = df.loc[emptiness_mask].drop('arguments', axis=1)        # Also drop the column 'arguments' completely
    records_args = df_args.to_records(index=False).tolist()
    records_no_args = df_no_args.to_records(index=False).tolist()

    conn = pg_hook.get_conn()
    cur = conn.cursor()
    query = cur.mogrify(f"""INSERT INTO templates ({','.join(columns)}) VALUES %s ON CONFLICT DO NOTHING""")
    execute_values(cur=cur, sql=query, argslist=records_args)
    conn.commit()

    log.info(f'Wrote {len(records_args)} entries with arguments for repository {repo["url"]} with ID {repo["repo_id"]}')

    columns.remove('arguments')
    query = cur.mogrify(f"""INSERT INTO templates ({','.join(columns)}) VALUES %s ON CONFLICT DO NOTHING""")
    execute_values(cur=cur, sql=query, argslist=records_no_args)
    conn.commit()

    log.info(f'Wrote {len(records_no_args)} entries with NO arguments for repository'
             f'{repo["url"]} with ID {repo["repo_id"]}')

    _finish_hook(pg_hook, success=True, repo_id=repo['repo_id'])
    log.info(f'Finished cleaning up, have a nice day')


dag = DAG('log2vec_process_repository',
          description='Processes a complete repository, by detecting logging, downloading repos, extracting source code'
                      'and parse them.',
          schedule_interval='*/30 * * * *',
          default_args={'db_conn_id': 'templates'},
          start_date=datetime(2020, 3, 20), catchup=False)

load_task = PythonOperator(task_id='load_from_database_task', dag=dag, python_callable=_load_from_database,
                           provide_context=True, params=default_args)
detect_task = PythonOperator(task_id='detect_task', dag=dag, python_callable=_detect,
                             provide_context=True, params=default_args)
clone_task = PythonOperator(task_id='clone_task', dag=dag, python_callable=_clone,
                            provide_context=True, params=default_args)
determine_framework_task = PythonOperator(task_id='determine_framework_task', dag=dag,
                                          python_callable=_determine_framework,
                                          provide_context=True, params=default_args)
extract_task = PythonOperator(task_id='extract_task', dag=dag, python_callable=_extract,
                              provide_context=True, params=default_args)
parse_task = PythonOperator(task_id='parse_task', dag=dag, python_callable=_parse,
                            provide_context=True, params=default_args)
formalize_task = PythonOperator(task_id='formalize_task', dag=dag, python_callable=_formalize,
                                provide_context=True, params=default_args)
update_task = PythonOperator(task_id='update_database_task', dag=dag, python_callable=_update_database,
                             provide_context=True, params=default_args)

load_task >> detect_task >> clone_task >> determine_framework_task >> extract_task >> parse_task >> formalize_task >> update_task
