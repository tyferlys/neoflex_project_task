import datetime
import logging
from io import StringIO

import pandas
import pandas as pd
from airflow.models.dag import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator

from includes.utils.load_data_to_db import load_data_to_db
from includes.decorators.logging_dag import logging_dag
from includes.constant import metadata_dag_dict_currency as metadata
from includes.utils.transform_date_to_db import transform_data_to_db


"""
    Это вариант с одним таском, вместо трех (extract, transform, load)
    В остальных файлах остались три эти такси
    Среднее время выполнения с одним таксом - около 3 секунд
"""
@logging_dag
def process_task(**kwargs):
    df: pandas.DataFrame = pd.read_csv(metadata["directory_to_file"], sep=",", dtype=metadata["dtype"])
    df = transform_data_to_db(df, metadata)

    metadata["dtype"] = {key.lower(): value for key, value in metadata["dtype"].items()}

    load_data_to_db(df, metadata, False, "project2-neoflex")


@logging_dag
def empty_function(**kwargs):
    pass


with DAG(
        "etl_dag_dict_currency",
        default_args={},
        description="Загрузка dict_currency",
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
        task_id="etl_process",
        python_callable=process_task,
    )

    end_etl = PythonOperator(
        task_id="etl_end",
        python_callable=empty_function
    )

    begin_etl >> process_task >> end_etl


if __name__ == "__main__":
    run = dag.test()