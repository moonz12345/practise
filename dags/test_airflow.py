# dags/test_dag.py
from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator

def hello_func():
    print("Hello from Docker!")

with DAG(
    'hello_dag',
    start_date=datetime(2025,1,1),
    schedule_interval=None,
    catchup=False  # рекомендуем добавить
) as dag:
    hello_task = PythonOperator(
        task_id='hello_task',  # исправлено task_id
        python_callable=hello_func  # исправлено на hello_func
    )