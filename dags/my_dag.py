from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime


def print_a():
    print("Hello from task_a")

def print_b():
    print("Hello from task_b")

# Set default params for all tasks in a DAG
default_args = {
    'retries': 3
}

with DAG(
    'my_dag',
    description='A demo DAG - hello airflow',
    schedule='@daily',
    start_date=datetime(2024,9,15),
    default_args=default_args,
    catchup=False,
    tags=['production', 'data_science']):

    task_a = PythonOperator(task_id='task_a', python_callable=print_a)
    task_b = PythonOperator(task_id='task_b', python_callable=print_b)

    # Set dependencies b/w tasks
    task_a >> task_b
    

