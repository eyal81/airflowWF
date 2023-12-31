from datetime import timedelta, datetime
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago
from airflow.models import Variable
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
import logging
from email.mime.image import MIMEImage

SUBJECT = "Hi"
SENDER = "eyalm@worksfusion.com"
RECIEPIENTS = ["eyal.moses81@gmail.com"]
PASSWORD = "czrw uchf hoqs sdno"
BODY = "TEST MAIL"

# op_kwargs = {'subject': SUBJECT, 'body': BODY, 'sender': SENDER, 'recipients': RECIEPIENTS,
#              'password': PASSWORD}

default_args = {
    "depends_on_past": False,
    "email": ["eyalm@worksfusion.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5)
}


def send_email(**kwargs):
    msg = MIMEMultipart('alternative')
    part2 = MIMEText(kwargs['body'], 'html')
    msg.attach(part2)
    msg['Subject'] = kwargs['subject']
    msg['From'] = kwargs['sender']
    msg['To'] = ', '.join(kwargs['recipients'])
    with smtplib.SMTP_SSL('smtp.gmail.com', 465) as smtp_server:
        smtp_server.login(kwargs['sender'], kwargs['password'])
        smtp_server.sendmail(kwargs['sender'], kwargs['recipients'], msg.as_string())
    logging.info("Message sent!")


dag = DAG(
    'send_user_report',
    default_args=default_args,
    description='Daily Active Users',
    schedule=timedelta(days=1),
    start_date=datetime(2021, 1, 1),
    catchup=False,
    tags=["reports"]
)

# define the first task
report = PythonOperator(
    task_id='send_report',
    python_callable=send_email,
    op_kwargs={'subject': SUBJECT, 'body': BODY, 'sender': SENDER, 'recipients': RECIEPIENTS,
               'password': PASSWORD},
    dag=dag,
)

report
