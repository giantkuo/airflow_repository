import os
import pendulum
from airflow.decorators import dag, task
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator

local_tz = pendulum.timezone("Asia/Taipei")

def check_file():
    filename = f'data_tmp/{pendulum.yesterday().strftime("%y-%m-%d")}.csv'
    if os.path.exists(filename):
        return "is_exist"
    else:   
        return "is_not_exist"
    
    
filename = f'data_tmp/{pendulum.yesterday().strftime("%y-%m-%d")}.csv'
default_args = {
    'owner': 'Allen Hsieh',
    'start_date': pendulum.datetime(2023, 12, 6, tz=local_tz),
    'schedule': '@daily',
    'retries': 1,
    'retry_delay': pendulum.duration(minutes=3)
}


with DAG(
    dag_id='hugreen_upload',
    default_args=default_args,
    description='Auto upload data to NAS',
    schedule='0 5 * * *'
) as dag:
    check_file_task = PythonOperator(
        task_id='check_file',
        python_callable=check_file
    )


    is_exist = DummyOperator(
        task_id='is_exist'
    )

    is_not_exist = DummyOperator(
        task_id='is_not_exist'
    )

    transmit_task = BashOperator(
        task_id='transmit',
        bash_command='./bash_file/upload.sh'
    )
    fail_task = DummyOperator(
        task_id='fail'
    )

    start_task = DummyOperator(
        task_id='start'
    )

    end_task = DummyOperator(
        task_id='end',
        trigger_rule='one_success'
    )

    start_task >> check_file_task >> [is_exist, is_not_exist]
    is_exist >> transmit_task >> end_task
    is_not_exist >> fail_task >> end_task