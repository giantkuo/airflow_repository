import os
import smtplib
import pendulum
from email.message import EmailMessage
from airflow.decorators import dag, task

local_tz = pendulum.timezone("Asia/Taipei")
base_folder = "/mnt/nas-data/Animal/pig_video/andrew_test/"
hours_to_check = ["08", "09", "10", "11", "12", "13", "14", "15", "16", "17"]
recipient_email = "r12631026@ntu.edu.tw"


@dag(
    dag_id="auto_check",
    owner="Andrew Hsieh",
    start_date=pendulum.datetime(2021, 1, 1, tz=local_tz),
    schedule="@daily",
    catchup=False,
    retries=1,
)
def auto_check_flow():
    def check_folder(folder):
        try:
            file = os.listdir(folder)
            if not file:
                return "[{}] Empty folder.\n".format(folder[-8:-1])
            else:
                return "[{}] Good. {} files found.\n".format(folder[-8:-1], len(file))
        except FileNotFoundError:
            return "[{}] File not found.\n".format(folder[-8:-1])

    @task
    def check_all_folders():
        today = pendulum.today(local_tz).format("%Y-%m-%d")
        status_log = "\n{} Auto Check Results:\n\n".format(today)
        for hour in hours_to_check:
            status_log += check_folder(base_folder + "rpi_2/{}/{}/".format(today, hour))
            status_log += check_folder(base_folder + "rpi_3/{}/{}/".format(today, hour))
        return status_log

    @task
    def email(subject, body, to_email):
        sender_email = "dummydumdum375@gmail.com"
        sender_password = "wlqx epfd hqlc ywnu"

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

    email(
        subject="[{}] Rpi camera system auto check".format(
            pendulum.today(local_tz).format("%Y-%m-%d")
        ),
        body=check_all_folders(),
        to_email=recipient_email,
    )


auto_check_flow()
