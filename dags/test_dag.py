# dags/pandas_dag.py
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

def pandas_analysis():
    try:
        import pandas as pd
        import numpy as np
        
        print("✅ Pandas доступен!")
        
        # Создаем тестовые данные
        data = {
            'product': ['A', 'B', 'C', 'A', 'B'],
            'sales': [100, 200, 150, 300, 250],
            'price': [10.5, 25.0, 15.3, 10.5, 25.0]
        }
        
        df = pd.DataFrame(data)
        print(f"📊 Данные:\n{df}")
        print(f"💰 Общие продажи: {df['sales'].sum()}")
        print(f"📈 Средняя цена: {df['price'].mean():.2f}")
        
    except ImportError as e:
        print(f"❌ Ошибка: {e}")

with DAG(
    dag_id='pandas_test',
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,
    catchup=False
) as dag:
    
    test_task = PythonOperator(
        task_id='pandas_analysis',
        python_callable=pandas_analysis
    )