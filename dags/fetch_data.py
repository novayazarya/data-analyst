from datetime import timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.dummy import DummyOperator
from airflow.utils.dates import days_ago

args = {'owner': 'airflow'}

with DAG(
    dag_id='fetch_data',
    default_args=args,
    schedule_interval='@daily',
    start_date=days_ago(0),
    dagrun_timeout=timedelta(minutes=60),
    tags=['bash', 'parse', 'python', 'dump', 'postgresql']
) as dag:

    fetch_data = BashOperator(
        task_id='fetching_data',
        bash_command='python3 $HOME/data_analyst_vacncies/parser.py',
    )
   
    dump_db = BashOperator(
        task_id='dumping_db',
        bash_command='pg_dump -c hh -f $HOME/data_analyst_vacncies/db/hh.bak',
    )
    
    fetch_data >> dump_db

if __name__ == "__main__":
        dag.cli()
