from datetime import timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.dummy import DummyOperator
from airflow.utils.dates import days_ago

args = {'owner': 'airflow'}

with DAG(
    dag_id='etl_data',
    default_args=args,
    schedule_interval='@weekly',
    start_date=days_ago(0),
    dagrun_timeout=timedelta(minutes=60),
    tags=['bash']
) as dag:

    extract_data = BashOperator(
        task_id='extracting_data',
        bash_command='python3 $HOME/data_analyst_vacncies/parser.py',
    )

    send_data = BashOperator(
        task_id='sending_data',
        bash_command='scp $HOME/data_analyst_vacncies/data.csv root@77.246.144.180:/var/www/enzo/data/www/popov.mn/data-analyst/src/files/aee5d40e70ea9830c96efe6da03ad32187ff7223ad1b7b84e38c32127ccf6661b576fe0005b42657703e7bfaaefabc74550268cc35f64122a652fc471110c832',
    )

    extract_data >> send_data

if __name__ == "__main__":
        dag.cli()
