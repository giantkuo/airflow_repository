import time
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator


default_args = {
    "owner": "Allen Hsieh",
    "depends_on_past": False,
    'start_date': datetime(2100, 1, 1, 0, 0),
    # "start_date": pendulum.datetime(2021, 1, 1, tz=local_tz),
    "schedule_interval": "@daily",
    "catchup": False,
    "retries": 1,
    # "retry_delay": pendulum.duration(minutes=1),
}

def auto_request_flow():
    print('Hello World!')
    time.sleep(5)
    print('Hello Again!')
    
    
with DAG('test_allen', default_args=default_args, catchup=False) as dag:
    auto_request_flow = PythonOperator(
        task_id='auto_request_flow',
        python_callable=auto_request_flow,
        dag=dag
    )
    auto_request_flow
