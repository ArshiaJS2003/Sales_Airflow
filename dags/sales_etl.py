from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import pandas as pd
import random
import matplotlib.pyplot as plt

def extract():
    products = ["Laptop", "Phone", "Tablet"]
    data = {
        "Product": [random.choice(products) for _ in range(10)],
        "Quantity": [random.randint(1, 5) for _ in range(10)],
        "Price": [random.choice([15000, 20000, 50000]) for _ in range(10)]
    }
    df = pd.DataFrame(data)
    return df.to_json()

def transform(ti):
    df = pd.read_json(ti.xcom_pull(task_ids='extract_task'))
    df["Revenue"] = df["Quantity"] * df["Price"]
    summary = df.groupby("Product")["Revenue"].sum().reset_index()
    return summary.to_json()

def visualize(ti):
    df = pd.read_json(ti.xcom_pull(task_ids='transform_task'))
    plt.figure()
    plt.bar(df["Product"], df["Revenue"])
    plt.savefig("include/chart.png")

with DAG(
    dag_id='sales_etl_pipeline',
    start_date=datetime(2024, 1, 1),
    schedule='@hourly',
    catchup=False
) as dag:

    extract_task = PythonOperator(
        task_id='extract_task',
        python_callable=extract
    )

    transform_task = PythonOperator(
        task_id='transform_task',
        python_callable=transform
    )

    visualize_task = PythonOperator(
        task_id='visualize_task',
        python_callable=visualize
    )

    extract_task >> transform_task >> visualize_task
