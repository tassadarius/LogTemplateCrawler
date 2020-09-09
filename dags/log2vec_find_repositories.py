from datetime import datetime
from airflow import DAG
from templatecrawler.airflow.plugins.operators import SearchRepoOperator, FilterSearchOperator

default_args = {
    'start_over': False,
}

dag = DAG('log2vec_find-repositories',
          description='Searches through the GitHub API for repositores and saves them to the database',
          schedule_interval='*/2 * * * *',
          default_args=default_args,
          start_date=datetime(2020, 3, 20), catchup=False)

start_over = False
find_task = SearchRepoOperator(task_id='find_repos_task', postgres_conn_id='templates',
                               start_over=start_over, dag=dag)
process_and_save_task = FilterSearchOperator(task_id='process_and_save_repos_task', postgres_conn_id='templates',
                                             language='random', dag=dag)

find_task >> process_and_save_task

