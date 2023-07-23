import psycopg2
import pandas as pd
import numpy as np

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

from datetime import datetime

from step_1_insert_data import *
from step_2_compare_price import *


def test_connection():
    conn = psycopg2.connect("host=postgres port=5432 dbname=airflow user=airflow password=airflow")
    print('Success : ' + str(conn))

dag_detail = {
    'owner': 'Pitchakorn_Prasertmet',
    'retries': 3,
}

with DAG(
    dag_id='BigC_assignment',
    default_args=dag_detail,
    description='Pipeline for BigC Assignment',
    start_date=datetime(2023,7,21,2),
    schedule_interval='@daily'

) as dag:
    task1 = PythonOperator(
        task_id='test_connection',
        python_callable=test_connection
    )

    task2 = PythonOperator(
        task_id='step_1_insert_data',
        python_callable=insert_data
    )

    task3 = PythonOperator(
        task_id='step_2_compare_price',
        python_callable=compare_price
    )

    task1 >> task2 >> task3