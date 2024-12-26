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
from includes.constant import metadata_dag_posting as metadata
from includes.utils.transform_date_to_db import transform_data_to_db

@logging_dag
def extract_file(**kwargs):
    df: pandas.DataFrame = pd.read_csv(metadata["directory_to_file"], sep=";", dtype=metadata["dtype"])

    kwargs["ti"].xcom_push(key="csv_data_json", value=df.to_json())
    logging.info(df.head())


@logging_dag
def transform_data(**kwargs):
    csv_data_json = kwargs["ti"].xcom_pull(key="csv_data_json")
    df: pandas.DataFrame = pd.read_json(StringIO(csv_data_json), dtype=metadata["dtype"])

    df = transform_data_to_db(df, metadata)
    kwargs["ti"].xcom_push(key="csv_data_transformed_json", value=df.to_json())
    logging.info(df.head())


@logging_dag
def load_data(**kwargs):
    csv_data_transformed_json = kwargs["ti"].xcom_pull(key="csv_data_transformed_json")
    metadata["dtype"] = {key.lower(): value for key, value in metadata["dtype"].items()}
    df: pandas.DataFrame = pd.read_json(StringIO(csv_data_transformed_json), dtype=metadata["dtype"])

    load_data_to_db(df, metadata, False)
    logging.info("Данные успешно загружены в бд")


@logging_dag
def empty_function(**kwargs):
    pass


with DAG(
        "etl_ft_posting",
        default_args={},
        description="ETL process for csv file ft_posting",
        start_date=datetime.datetime(2024, 12, 20),
        schedule_interval=None,
        catchup=False,
        tags=["study"],
) as dag:
    begin_etl = PythonOperator(
        task_id="etl_start",
        python_callable=empty_function
    )

    sql_script_init = SQLExecuteQueryOperator(
        task_id="init_table",
        sql=metadata["sql_script"],
        conn_id="project-neoflex-db"
    )

    extract_task = PythonOperator(
        task_id="etl_extract",
        python_callable=extract_file,
    )

    transform_data_task = PythonOperator(
        task_id="etl_transform",
        python_callable=transform_data
    )

    load_data_task = PythonOperator(
        task_id="etl_load",
        python_callable=load_data
    )

    end_etl = PythonOperator(
        task_id="etl_end",
        python_callable=empty_function
    )

    begin_etl >> sql_script_init >> extract_task >> transform_data_task >> load_data_task >> end_etl

if __name__ == "__main__":
    run = dag.test()
