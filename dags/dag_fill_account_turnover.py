import datetime
import logging
from io import StringIO

import pandas
import pandas as pd
from airflow.models.dag import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

from includes.utils.load_data_to_db import load_data_to_db
from includes.decorators.logging_dag import logging_dag
from includes.constant import metadata_dag_posting as metadata
from includes.utils.transform_date_to_db import transform_data_to_db


def process_task(**kwargs):
    postgres_hook: PostgresHook = PostgresHook("project-neoflex-db")
    postgres_hook.run(
        "CALL ds.fill_account_turnover_f(%(date_fill)s)",
        parameters=kwargs['dag_run'].conf
    )


with DAG(
    "fill_account_turnover",
    default_args={},
    description="Заполнение витрины account_turnover",
    start_date=datetime.datetime(2024, 12, 20),
    schedule_interval=None,
    catchup=False,
    max_active_runs=1,
    tags=["study"],
    params={
        "date_fill": datetime.date.today()
    }
) as dag:
    process_task = PythonOperator(
        task_id="fill_process",
        python_callable=process_task
    )

    process_task


if __name__ == "__main__":
    run = dag.test()
