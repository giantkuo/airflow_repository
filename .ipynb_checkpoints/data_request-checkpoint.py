# -*- coding: utf-8 -*-
# +
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
import requests
from requests import get
import hashlib
import json
from datetime import datetime
import time
import csv
import os
import glob
import datetime
import pytz


def insertData(mydb, cursor, tb, key, format, data):
    sqlFormula = f"INSERT INTO {tb} {key} VALUES {format}"
    cursor.executemany(sqlFormula, data)
    mydb.commit()
    return cursor


def md5(text: str):
    """MD5加密"""
    return hashlib.md5(text.encode()).hexdigest()


def generate_token():
    pw = md5('sensor@TOOLMEN')
    datas = {'key': 'toolmen', 'secret': pw}

    r = requests.post('https://gnomeapi.fufluns.net/user/getAccessToken', data=datas)
    r = r.json()
    accessToken = r["data"]["accessToken"]
    
    
def hubID(**kwargs):
    ti = kwargs['ti']
    accessToken = ti.xcom_pull(task_ids='generate_token')
    datas = {'accessToken': accessToken}
    r = requests.post('https://gnomeapi.fufluns.net/hub/getBindInfo', data=datas)
    jsonFile = r.json()
    hubId = jsonFile['data'][0]['hubId']
    return hubId
    
def request_data():
    # Hub history data
    ti = kwargs['ti']
    accessToken = ti.xcom_pull(task_ids='generate_token')
    
    start_time = convert_taipei_time_to_unix('2024-01-15 23:59:59')
    end_time = convert_taipei_time_to_unix('2024-01-01 23:59:59')

    # Request data
    datas = {'accessToken': accessToken,
             'hubId': "3120317e",
             'endTime': end_time,
             'startTime': start_time,
             'interval': 2
            }

    r = requests.post('https://gnomeapi.fufluns.net/hub/getSensorHistory', data=datas)
    jsonFile = r.json()
    values = []
    for i in range(len(jsonFile['data'])):
        try:
            date = jsonFile['data'][i]['date']
            date = datetime.fromtimestamp(date)
            hub_sensor = jsonFile['data'][i]['sensors']
            tmp = hub_sensor[2]['data'][0]
            humidity = hub_sensor[2]['data'][1]
            co2 = hub_sensor[0]['data'][0]
            nh3 = hub_sensor[1]['data'][0]

            # Write to the csv file
            data_row = (date.strftime("%y-%m-%d"), date.strftime('%H:%M:%S'), tmp, humidity, co2, nh3)
            values.append(data_row)
            
    mydb = pymysql.connect(
        # DNS: mysql-service.yun-personal
        host="10.233.22.60",
        user="root",
        password="Allenkk@910305",
        port=3306,
        charset="utf8",
        cursorclass=pymysql.cursors.DictCursor,
        database="chicken"
    )
    print("Successfully connected!")
    cursor = mydb.cursor()
    key = '(data, time, temp, humidity, co2, nh3)'
    value_format = "(%s, %s, %s, %s, %s, %s)"
    insertData(mydb, cursor, "sensor", key, value_format, values)
    

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
        
        
def convert_taipei_time_to_unix(taipei_time_str):
    taipei_tz = pytz.timezone('Asia/Taipei')

    dt = datetime.datetime.strptime(taipei_time_str, '%Y-%m-%d %H:%M:%S')

    taipei_dt = taipei_tz.localize(dt)

    unix_timestamp = int(taipei_dt.timestamp())

    return unix_timestamp



# +
with DAG('store_sensor', default_args=default_args) as dag:

    generate_token = PythonOperator(
        task_id = 'generate_token',
        python_callable = generate_token,
    )
    
#     get_hubID = PythonOperator(
#         task_id = 'get_hubID',
#         python_callable = hubID,
#         provide_context=True,
#     )
    
    request_data = PythonOperator(
        task_id = 'request_data',
        python_callable = request_data,
        provide_context=True,
    )
    
    get_read_history = PythonOperator(
        task_id='get_read_history',
        python_callable=process_metadata,
        op_args=['read']
    )

    check_comic_info = PythonOperator(
        task_id='check_comic_info',
        python_callable=check_comic_info,
        provide_context=True
    )

    decide_what_to_do = BranchPythonOperator(
        task_id='new_comic_available',
        python_callable=decide_what_to_do,
        provide_context=True
    )

    update_read_history = PythonOperator(
        task_id='update_read_history',
        python_callable=process_metadata,
        op_args=['write'],
        provide_context=True
    )

    generate_notification = PythonOperator(
        task_id='yes_generate_notification',
        python_callable=generate_message,
        provide_context=True
    )

    send_notification = SlackAPIPostOperator(
        task_id='send_notification',
        token="YOUR_SLACK_TOKEN",
        channel='#comic-notification',
        text="[{{ ds }}] 海賊王有新番了!",
        icon_url='http://airbnb.io/img/projects/airflow3.png'
    )

    do_nothing = DummyOperator(task_id='no_do_nothing')

    # define workflow
    get_read_history >> check_comic_info >> decide_what_to_do

    decide_what_to_do >> generate_notification
    decide_what_to_do >> do_nothing

    generate_notification >> send_notification >> update_read_history
