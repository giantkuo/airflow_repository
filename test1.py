from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import BranchPythonOperator, PythonOperator
import os, airflow
from airflow.utils.dates import days_ago
from airflow.operators.email import EmailOperator
import pendulum
import time

local_tz = pendulum.timezone("Asia/Taipei")

default_args = {
    'owner': 'someone',
    'depends_on_past': False,
    #'start_date': airflow.utils.dates.days_ago(1),
    'email': ['toolmen@gmail.com'],
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 1,
    'retry_delay': timedelta(seconds=30),
}

with DAG(
    dag_id='example_for_sent_mail',
    default_args=default_args,
    description='A simple tutorial DAG',
    schedule_interval='0 0 * * *',
    start_date=days_ago(2),
    catchup=False, # missing a dag not run again
    tags=['example'],
    max_active_runs=1, # Follow a sequence to avoid deadlock when using MySQL
) as dag:

    email_task = EmailOperator(
        task_id='send_email',
        to='toolmenshare@gmail.com',
        subject='Airflow success',
        html_content=""" <h3>Email Test</h3> {{ execution_date }}<br/>""",
        dag=dag
    )
    email_task
