from datetime import datetime
import pandas as pd
import logging
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.postgres_hook import PostgresHook
from psycopg2.extras import execute_values

from templatecrawler.airflow.plugins.operators import \
    CloneAndExtractOperator, FilterTemplatesOperator, ParseOperator, FormalizeOperator


default_args = {
    'postgres_conn_id': 'templates'
}

log = logging.getLogger(__name__)


# Also takes nested empty lists into account
# Taken from https://stackoverflow.com/a/1605679/7307284
def is_list_empty(inList):
    if isinstance(inList, list):  # Is a list
        return all(map(is_list_empty, inList))
    return False                  # Not a list


def _load_from_database(**context):
    params = context['params']
    postgres_conn_id = params['postgres_conn_id']
    pg_hook = PostgresHook(postgres_conn_id=postgres_conn_id)
    cur = pg_hook.get_cursor()
    table_name = 'repositories'
    constraint_col0 = 'processed'
    constraint_col1 = 'contains_logging'
    raw_query = f"""SELECT * from {table_name} WHERE {constraint_col0} = %s AND {constraint_col1} = %s  LIMIT 20"""
    query = cur.mogrify(raw_query)
    repos = pg_hook.get_pandas_df(query, parameters=[True, True])
    task_instance = context['task_instance']
    task_instance.xcom_push('target_repositories', repos)
    return True


def _update_database(**context):
    task_instance = context['task_instance']
    data = task_instance.xcom_pull(key='formalized')

    params = context['params']
    postgres_conn_id = params['postgres_conn_id']
    pg_hook = PostgresHook(postgres_conn_id=postgres_conn_id)

    data['parsed'] = True
    data['crawl_date'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    columns = ['template', 'arguments', 'raw', 'repo_id', 'parsed', 'crawl_date']
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
    table = 'templates'
    query = cur.mogrify(f"""INSERT INTO {table} ({','.join(columns)}) VALUES %s""")
    execute_values(cur=cur, sql=query, argslist=records_args)
    conn.commit()

    columns.remove('arguments')
    query = cur.mogrify(f"""INSERT INTO {table} ({','.join(columns)}) VALUES %s""")
    execute_values(cur=cur, sql=query, argslist=records_no_args)
    conn.commit()
    cur.close()


dag = DAG('log2vec_extract-and-parse',
          description='Takes entries from the database, downloads some files and checks if they contain logging',
          schedule_interval='*/30 * * * *',
          default_args={'db_conn_id': 'templates'},
          start_date=datetime(2020, 3, 20), catchup=False)

load_task = PythonOperator(task_id='load_from_database_task', dag=dag, python_callable=_load_from_database,
                           provide_context=True, params=default_args)
clone_extract_task = CloneAndExtractOperator(task_id='clone_and_extract_task', dag=dag)
parse_task = ParseOperator(task_id='parse_task', dag=dag)
filter_task = FilterTemplatesOperator(task_id='filter_task', dag=dag)
formalize_task = FormalizeOperator(task_id='formalize_task', dag=dag)
update_task = PythonOperator(task_id='update_database_task', dag=dag, python_callable=_update_database,
                             provide_context=True, params=default_args)
load_task >> clone_extract_task >> parse_task >> filter_task >> formalize_task >> update_task
