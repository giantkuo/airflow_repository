'''
Author: Allen
'''
import smtplib
import pendulum
from email.message import EmailMessage
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
import paramiko

local_tz = pendulum.timezone("Asia/Taipei")
base_folder = "/workspace/nas/Animal/optical_flow_chicken_video/"
hours_to_check = ["08", "09", "10", "11", "12", "13", "14", "15", "16", "17"]

default_args = {
    "owner": "Allen",
    "depends_on_past": False,
    "start_date": pendulum.datetime(2024, 2, 1, tz=local_tz),
    "schedule_interval": "0 20 * * *",
    "catchup": False,
    "retries": 1,
    "retry_delay": pendulum.duration(minutes=1),
}


def check_file():
    sftp_host = '140.112.183.104'
    sftp_port = 3040
    sftp_username = 'pokemon'
    sftp_password = 'lab304nas'
    remote_path = "/nas-data/Animal/optical_flow_chicken_video/{}/".format(
        pendulum.today(local_tz).format("YMMDD")
    )
    rpis = ['rpi1/L/', 'rpi1/R/', 'rpi3/R/', 'rpi3/L/']

    # establish SSH client
    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    ssh.connect(sftp_host, port=sftp_port, username=sftp_username, password=sftp_password)

    # establish SFTP connection
    sftp = ssh.open_sftp()
    global result
    today = pendulum.today(local_tz).format("Y-MM-DD")
    status_log = "\n{} Auto Check Results:\n\n".format(today)
    for rpi in rpis:
        try:
            new_remote_path = remote_path + rpi
            print(new_remote_path)
            files = sftp.listdir(new_remote_path)
            if files:
                result = "[{}] Good. {} files found.\n".format(new_remote_path[-16:-1], len(files))
            else:
                result = "[{}] Empty folder.\n".format(new_remote_path[-16:-1])
        except FileNotFoundError:
            result = "[{}] File not found.\n".format(new_remote_path[-16:-1])

        status_log += result

    # close connection
    sftp.close()
    ssh.close()
    print(status_log)
    return status_log


def email(**kwargs):
    ti = kwargs['ti']
    sender_email = "toolmenshare@gmail.com"
    sender_password = "nmuv xsgn sqfn lbmw"
    to_email = 'yylunxie@gmail.com'
    subject = "[{}] Rpi camera system auto check".format(
        pendulum.today(local_tz).format("Y-MM-DD")
    )
    body = ti.xcom_pull(task_ids='check_file')
    if body is not None:
        # Set up the email
        msg = EmailMessage()
        msg.set_content(body)
        msg["Subject"] = subject
        msg["From"] = sender_email
        msg["To"] = to_email

        # Connect to Gmail's SMTP server
        try:
            with smtplib.SMTP_SSL("smtp.gmail.com", 465) as smtp:
                smtp.login(sender_email, sender_password)
                smtp.send_message(msg)
            print("Email sent to {}".format(to_email))
        except Exception as e:
            print("Failed to send email: {}".format(e))
    else:
        print("Error: The email body is None.")


with DAG(
    "email_notification",
    default_args=default_args
) as dag:
    check_file_task = PythonOperator(
        task_id='check_file',
        python_callable=check_file,
        dag=dag,
    )
    email_task = PythonOperator(
        task_id="email",
        python_callable=email,
        dag=dag,
    )

    check_file_task >> email_task
