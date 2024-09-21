from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from datetime import datetime

# we can use ti for the task instance
def print_a(ti):
    print(f"Hello from task_a")
    ti.xcom_push(key='aval', value='001')

# or we can use kwargs
def print_b(**kwargs):
    aval = kwargs['ti'].xcom_pull(key='aval',task_ids='task_a')
    val = Variable.get('API_URL')
    return val + aval

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
    

