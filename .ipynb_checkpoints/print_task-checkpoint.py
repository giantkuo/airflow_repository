# +
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta

def my_print_function():
    print("Hello from Airflow!")

default_args = {
    'owner': 'admin',
    'start_date': datetime(2023, 12, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'schedule_intervel': "0 20 */15 * *"
}

dag = DAG(
    'print_task',
    default_args=default_args,
    description='A simple DAG that just prints a message',
    schedule_interval=timedelta(days=1),
)

print_task = PythonOperator(
    task_id='print_hello',
    python_callable=my_print_function,
    dag=dag,
)

print_task

