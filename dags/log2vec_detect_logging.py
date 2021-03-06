from datetime import datetime
import logging
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.postgres_hook import PostgresHook
from psycopg2.extras import execute_values

from templatecrawler.airflow.plugins.operators import  \
    FetchFilesOperator, DetectLoggingWithoutFilesOperator, DetectLoggingFromFilesOperator

default_args = {
    'postgres_conn_id': 'templates'
}

log = logging.getLogger(__name__)


def _load_from_database(**context):
    params = context['params']
    postgres_conn_id = params['postgres_conn_id']
    pg_hook = PostgresHook(postgres_conn_id=postgres_conn_id)
    table_name = 'repositories'
    constraint_column = 'processed'
    query = f"""SELECT * from {table_name} WHERE {constraint_column} = %s LIMIT 20"""
    repos = pg_hook.get_pandas_df(query, parameters=[False])
    task_instance = context['task_instance']
    task_instance.xcom_push('repositories', repos)
    return True


def _update_database(**context):
    task_instance = context['task_instance']
    from_files = task_instance.xcom_pull(key='logging_check_from_files')
    without_files = task_instance.xcom_pull(key='logging_check_without_files')
    records_framework = None
    if from_files is not None and without_files is not None:
        records_contains = list(from_files['contains_logging'].items())
        records_contains += list(without_files['contains_logging'].items())
        records_framework = list(from_files['framework'].items())
    elif from_files is not None:
        records_contains = list(from_files['contains_logging'].items())
        records_framework = list(from_files['framework'].items())
    elif without_files is not None:
        records_contains = list(without_files['contains_logging'].items())
    else:
        log.warning("Did not receive any data to update")
        return

    params = context['params']
    postgres_conn_id = params['postgres_conn_id']
    pg_hook = PostgresHook(postgres_conn_id=postgres_conn_id)
    table_name = 'repositories'
    constraint_col = 'repo_id'
    target_col = 'contains_logging'
    process_col = 'processed'
    conn = pg_hook.get_conn()
    cur = conn.cursor()
    query = cur.mogrify(f"""UPDATE {table_name} SET {target_col} = data.{target_col}, {process_col} = TRUE FROM
(VALUES %s) AS data({constraint_col}, {target_col}) WHERE {table_name}.{constraint_col} = data.{constraint_col}""")
    execute_values(cur=cur, sql=query, argslist=records_contains)
    conn.commit()
    if not records_framework:
        cur.close()
        return True
    target_col = 'framework'
    query = cur.mogrify(f"""UPDATE {table_name} SET {target_col} = data.{target_col} FROM
    (VALUES %s) AS data({constraint_col}, {target_col}) WHERE {table_name}.{constraint_col} = data.{constraint_col}""")
    execute_values(cur=cur, sql=query, argslist=records_framework)
    conn.commit()
    cur.close()
    return True


dag = DAG('log2vec_detect-logging',
          description='Takes entries from the database, downloads some files and checks if they contain logging',
          schedule_interval='*/10 * * * *',
          default_args={'db_conn_id': 'templates'},
          start_date=datetime(2020, 3, 20), catchup=False)

load_task = PythonOperator(task_id='load_from_database_task', dag=dag, python_callable=_load_from_database,
                           provide_context=True, params=default_args)
fetch_files_task = FetchFilesOperator(task_id='fetch_files_task')
detect_from_files_task = DetectLoggingFromFilesOperator(task_id='detect_from_files_task', dag=dag)
detect_without_files_task = DetectLoggingWithoutFilesOperator(task_id='detect_without_files_task', dag=dag)
update_task = PythonOperator(task_id='update_database_task', dag=dag, python_callable=_update_database,
                             provide_context=True, params=default_args)

load_task >> fetch_files_task >> [detect_from_files_task, detect_without_files_task] >> update_task
