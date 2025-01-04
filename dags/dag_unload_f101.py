import datetime
import logging
import pandas
import pandas as pd

from io import StringIO
from airflow.models.dag import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from includes.decorators.logging_dag import logging_dag
from includes.constant import metadata_f101_unload as metadata


@logging_dag
def process_task(**kwargs):
    postgres_hook: PostgresHook = PostgresHook("project-neoflex-db")
    df: pandas.DataFrame = pd.read_sql(
        f"SELECT * FROM {metadata["schema"]}.{metadata["tablename"]}",
        postgres_hook.get_conn(),
        dtype=metadata["dtype"]
    )
    df.to_csv(metadata["directory_to_file"], sep=";", index=False)


@logging_dag
def empty_function(**kwargs):
    pass


with DAG(
    "load_f101_to_csv",
    default_args={},
    description="Выгрузка отчета в csv файл",
    start_date=datetime.datetime(2024, 12, 20),
    schedule_interval=None,
    catchup=False,
    max_active_runs=1,
    tags=["study"],
) as dag:
    begin_etl = PythonOperator(
        task_id="etl_start",
        python_callable=empty_function
    )

    process_task = PythonOperator(
        task_id="unload_f101",
        python_callable=process_task,
    )

    end_etl = PythonOperator(
        task_id="etl_end",
        python_callable=empty_function
    )

    begin_etl >> process_task >> end_etl


if __name__ == "__main__":
    run = dag.test()