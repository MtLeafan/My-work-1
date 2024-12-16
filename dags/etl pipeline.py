from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago
import pandas as pd
import sqlite3

# Функция для извлечения данных
def extract_data():
    df = pd.read_csv('data/employees.csv')
    return df.to_json()

# Функция для преобразования данных
def transform_data(**kwargs):
    ti = kwargs['ti']
    data = ti.xcom_pull(task_ids='extract_data')
    df = pd.read_json(data)
    df = df[df['department'] == 'IT']
    return df.to_json()

# Функция для загрузки данных
def load_data(**kwargs):
    ti = kwargs['ti']
    data = ti.xcom_pull(task_ids='transform_data')
    df = pd.read_json(data)
    
    conn = sqlite3.connect('data/company.db')
    df.to_sql('employees', conn, if_exists='replace', index=False)
    conn.close()

# Определяем DAG
default_args = {
    'owner': 'airflow',
    'start_date': days_ago(1),
}

dag = DAG(
    'etl_pipeline',
    default_args=default_args,
    description='ETL Pipeline',
    schedule_interval='@daily',
)

# Задача 1: Извлечение данных
extract_task = PythonOperator(
    task_id='extract_data',
    python_callable=extract_data,
    dag=dag,
)

# Задача 2: Преобразование данных
transform_task = PythonOperator(
    task_id='transform_data',
    python_callable=transform_data,
    provide_context=True,
    dag=dag,
)

# Задача 3: Загрузка данных
load_task = PythonOperator(
    task_id='load_data',
    python_callable=load_data,
    provide_context=True,
    dag=dag,
)

# Устанавливаем порядок выполнения задач
extract_task >> transform_task >> load_task