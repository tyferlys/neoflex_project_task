from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from datetime import datetime

# Функция, которая будет вызвана в PythonTask
def print_hello():
    print("Hello from Airflow!")

# Определение DAG
default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'test_dag',
    default_args=default_args,
    description='A simple test DAG',
    schedule_interval=None,  # Вы можете поставить cron-выражение или None для ручного запуска
    start_date=datetime(2024, 12, 22),
    catchup=False,
) as dag:

    # Операции для выполнения

    start_task = DummyOperator(
        task_id='start',
    )

    print_task = PythonOperator(
        task_id='print_hello',
        python_callable=print_hello,
    )

    end_task = DummyOperator(
        task_id='end',
    )

    # Определение порядка выполнения задач
    start_task >> print_task >> end_task