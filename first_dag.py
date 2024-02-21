from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta


# 定義各個任務的動作
def extract():
    print("extract from data source")


def transform():
    print("transform data")  


def load():
    print("load data to the target")


# default_args = {
#     "owner": "Admin",
#     "depends_on_past": False,
#     "start_date": pendulum.datetime(2024, 1, 1, tz=local_tz),
#     "schedule_interval": "@daily",
#     "catchup": False,
#     "retries": 1,
#     "retry_delay": pendulum.duration(minutes=1),
# }


# 定義DAG
with DAG(
    "first_dag",
    default_args={
        "retries": 1,
        "retry_delay": timedelta(minutes=5),
    },
    description="my first DAG",
    schedule_interval=timedelta(days=1),
    start_date=datetime(2021, 1, 1),
    catchup=False,
) as dag:
    # 定義 Task
    extract_task = PythonOperator(
        task_id="extract", python_callable=extract, dag=dag,
    )
    transform_task = PythonOperator(
        task_id="transform", python_callable=transform, dag=dag,
    )
    load_task = PythonOperator(
        task_id="load", python_callable=load, dag=dag,
    )
    # 定義 Task 關聯關係
    extract_task >> transform_task >> load_task
