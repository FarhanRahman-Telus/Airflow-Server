from airflow import DAG
from airflow.operators import BashOperator,PythonOperator
import requests
import pandas as pd
from datetime import date, timedelta, datetime
import csv
import os
import time
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.mime.base import MIMEBase
from email import encoders
import urllib3
urllib3.disable_warnings()


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email': ['airflow@airflow.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
  }

dag = DAG('simple', default_args=default_args)

t1 = BashOperator(
    task_id='testairflow',
    bash_command='python /home/airflow/airflow/dags/scripts/capacity_build_plan_ranking_quarterly_email_reports.py',
    dag=dag)
     
