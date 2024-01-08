import os
import pendulum
from airflow.decorators import dag, task
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator


local_tz = pendulum.timezone("Asia/Taipei")
# git

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

dag = DAG(
    dag_id='allen_hugreen_upload',
    description='Auto upload data to NAS',
    default_args=default_args,
    schedule='0 5 * * *'
)

check_file_task = PythonOperator(
    task_id='check_file',
    python_callable=check_file,
    dag=dag
)

is_exist = DummyOperator(
    task_id='is_exist',
    dag=dag
)

is_not_exist = DummyOperator(
    task_id='is_not_exist',
    dag=dag
)

transmit_task = BashOperator(
    task_id='transmit',
    bash_command='./bash_file/upload.sh',
    dag=dag
)

fail_task = DummyOperator(
    task_id='fail',
    dag=dag
)

start_task = DummyOperator(
    task_id='start',
    dag=dag
)

end_task = DummyOperator(
    task_id='end',
    dag=dag,
    trigger_rule='one_success'
)

start_task >> check_file_task >> [is_exist, is_not_exist]
is_exist >> transmit_task >> end_task
is_not_exist >> fail_task >> end_task
