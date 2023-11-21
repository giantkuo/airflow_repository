import os
import smtplib
from datetime import datetime, timedelta
from email.message import EmailMessage
from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.python_operator import PythonOperator


today_date = datetime.today().date().strftime("%Y-%m-%d")
base_folder = "/mnt/nas-data/Animal/pig_video/andrew_test/"
hours_to_check = ["08", "09", "10", "11", "12", "13", "14", "15", "16", "17"]
recipient_email = "r12631026@ntu.edu.tw"
alert_message_body = "\n{} Auto Check Results:\n\n".format(today_date)

default_args = {
    "owner": "Andrew Hsieh",
    "start_date": days_ago(1),
    "schedule_interval": "0 19 * * *",
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
    "catchup": False,
}


def check_folder(folder):
    try:
        if not os.listdir(folder):
            alert("Empty folder", folder)
        else:
            alert("Good", folder)
    except FileNotFoundError:
        alert("File not found", folder)


def send_email(subject="Test", body="Test", to_email=recipient_email):
    # Sender details
    sender_email = "dummydumdum375@gmail.com"
    sender_password = "wlqx epfd hqlc ywnu"

    # Set up the email
    msg = EmailMessage()
    msg.set_content(body)
    msg["Subject"] = subject
    msg["From"] = sender_email
    msg["To"] = to_email

    # Connect to Gmail's SMTP server
    with smtplib.SMTP_SSL("smtp.gmail.com", 465) as smtp:
        smtp.login(sender_email, sender_password)
        smtp.send_message(msg)

    print("Email sent to {}".format(to_email))


def alert(error, folder):
    global alert_message_body
    folder = folder[39:]  # remove base_folder
    if error == "Empty folder":
        alert_message_body += "[{}] Empty folder. Camera may be down.\n".format(folder)
    elif error == "File not found":
        alert_message_body += (
            "[{}] File not found. Internet connection may be down.\n".format(folder)
        )
    elif error == "Good":
        alert_message_body += "[{}] Good.\n".format(folder)


def check():
    # check folder from 8am to 17pm for rpi2 and rpi3
    for hour in hours_to_check:
        check_folder(base_folder + "rpi_2/{}/{}/".format(today_date, hour))
        check_folder(base_folder + "rpi_3/{}/{}/".format(today_date, hour))


def email():
    send_email(
        subject="[{}] Rpi camera system auto check".format(today_date),
        body=alert_message_body,
    )


with DAG("auto_check", default_args=default_args) as dag:
    check_task = PythonOperator(task_id="check", python_callable=check)

    email_task = PythonOperator(task_id="send_email", python_callable=email)

    check_task >> email_task
