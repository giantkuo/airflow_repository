import os
import pendulum
import hashlib
import requests
import csv
from email.message import EmailMessage
from airflow.decorators import dag, task
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator


local_tz = pendulum.timezone("Asia/Taipei")


def data_request():
    
    def md5(text: str):
        """MD5加密"""
        return hashlib.md5(text.encode()).hexdigest()

    
    def create_csv_if_not_exists(file_name, header):
        """
        Create a CSV file with a specified header if it does not already exist.

        Args:
        file_name (str): The name of the CSV file.
        header (list): A list of column headers for the CSV file.
        """
        if not os.path.exists(file_name):
            with open(file_name, 'w', newline='') as file:
                writer = csv.writer(file)
                writer.writerow(header)
        else:
            print(f"File '{file_name}' already exists.")
            
            
    def append_to_csv(file_name, data_row):
        """
        Append a row to an existing CSV file.

        Args:
        file_name (str): The name of the CSV file.
        data_row (list): A list of values to append as a new row in the file.
        """
        with open(file_name, 'a', newline='') as file:
            writer = csv.writer(file)
            writer.writerow(data_row)
    
    # Token
    # secret need to be turned into md5
    pw = md5('sensor@TOOLMEN')
    datas = {'key': 'toolmen', 'secret': pw}

    r = requests.post('https://gnomeapi.fufluns.net/user/getAccessToken', data=datas)
    r = r.json()
    accessToken = r["data"]["accessToken"]
    # Hub

    datas = {'accessToken': accessToken}

    r = requests.post('https://gnomeapi.fufluns.net/hub/getBindInfo', data=datas)
    jsonFile = r.json()

    date = jsonFile['data'][0]['lastMonitor']['date']
    date = pendulum.datetime.fromtimestamp(date)
    c = pendulum.datetime.now()
    # print(f'date: {date}')

    # Create a csv file
    file_name = f'data_tmp/{date.strftime("%y-%m-%d")}.csv'  # e.g. 20-12-31.csv  
    header = ['Current time', 'Data Acquire Time', 'Temperature', 'Humidity', 'CO2', 'NH3']
    create_csv_if_not_exists(file_name, header)

    # Write in the sensors data
    hub_sensor = jsonFile['data'][0]['lastMonitor']['sensors']
    tmp = hub_sensor[2]['data'][0]
    humidity = hub_sensor[2]['data'][1]
    co2 = hub_sensor[0]['data'][0]
    nh3 = hub_sensor[1]['data'][0]

    data_row = [c.strftime('%H:%M:%S'), date.strftime('%H:%M:%S'), tmp, humidity, co2, nh3]  # Replace with the data you want to append
    append_to_csv(file_name, data_row)
    


filename = f'data_tmp/{pendulum.yesterday().strftime("%y-%m-%d")}.csv'
default_args = {
    'owner': 'Allen Hsieh',
    'start_date': pendulum.datetime(2021, 1, 1, tz=local_tz),
    'schedule_interval': '@daily',
    'retries': 1,
    'retry_delay': pendulum.duration(minutes=3)
}

dag = DAG(
    dag_id='allen_hugreen_request',
    description='Auto request data from hugreen sensors',
    default_args=default_args,
    schedule_interval='*/5 6-18 * * *'
)

data_request_task = PythonOperator(
    task_id='data_request',
    python_callable=data_request,
    dag=dag,
    trigger_rule='one_success'
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

start_task >> data_request_task >> end_task