from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator

with DAG(
    'check_dag',
    start_date=datetime(2024,1,1),
    schedule='@daily',
    catchup=False,
    description='DAG to check data',
    tags=['data_engineering']):
    
    create_file = BashOperator(task_id='create_file', bash_command='echo "Hi there!" >/tmp/dummy')
    check_file_exists = BashOperator(task_id='check_file', bash_command='test -f /tmp/dummy')
    read_file = PythonOperator(task_id='read_file', python_callable=lambda: print(open('/tmp/dummy', 'rb').read()))

    create_file >> check_file_exists >> read_file