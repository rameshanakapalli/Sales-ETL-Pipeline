from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import pandas as pd
import sqlite3

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'sales_etl_pipeline',
    default_args=default_args,
    description='ETL pipeline for sales data using Airflow',
    schedule_interval='@daily',
    start_date=datetime(2025, 11, 25),
    catchup=False
)

def extract(**kwargs):
    file_path = r'C:\Users\Koushik\Downloads\Sales_April_2019.csv'
    df = pd.read_csv(file_path)
    df.to_csv(r'C:\Users\Koushik\Downloads\sales_extracted.csv', index=False)
    print("Extraction complete, rows:", len(df))

def transform(**kwargs):
    df = pd.read_csv(r'C:\Users\Koushik\Downloads\sales_extracted.csv')
    df = df.drop_duplicates()
    df['Quantity'] = df['Quantity'].fillna(0)
    df['UnitPrice'] = df['UnitPrice'].fillna(df['UnitPrice'].mean())
    df['TotalAmount'] = df['Quantity'] * df['UnitPrice']
    df['Date'] = pd.to_datetime(df['Date'], errors='coerce')
    df.to_csv(r'C:\Users\Koushik\Downloads\sales_transformed.csv', index=False)
    print("Transformation complete")

def load(**kwargs):
    df = pd.read_csv(r'C:\Users\Koushik\Downloads\sales_transformed.csv')
    conn = sqlite3.connect(r'C:\Users\Koushik\Downloads\sales_data.db')
    df.to_sql('sales', conn, if_exists='replace', index=False)
    conn.close()
    print("Load complete into SQLite database")

extract_task = PythonOperator(
    task_id='extract_data',
    python_callable=extract,
    dag=dag
)

transform_task = PythonOperator(
    task_id='transform_data',
    python_callable=transform,
    dag=dag
)

load_task = PythonOperator(
    task_id='load_data',
    python_callable=load,
    dag=dag
)

extract_task >> transform_task >> load_task
