import datetime
import logging
from functools import wraps

from airflow.providers.postgres.hooks.postgres import PostgresHook


def logging_dag(func):
    @wraps(func)
    def wrapper(*args, **kwargs):
        dag_id = kwargs['dag'].dag_id
        task_id = kwargs['task_instance'].task_id
        start_time = kwargs['ti'].xcom_pull(key='start_time')
        current_time = datetime.datetime.now()

        postgres_hook = PostgresHook("project-neoflex-db")

        if task_id == "etl_start":
            with postgres_hook.get_sqlalchemy_engine().connect() as connection:
                connection.execute(
                    """
                        INSERT INTO logs.dags_logs(dag_id, time_start, time_end, status, error_message)
                        VALUES (%s, %s, %s, %s, %s)
                    """,
                    (dag_id, current_time, None, "Старт", None)
                )
            kwargs['ti'].xcom_push(key='start_time', value=current_time)
            return
        elif task_id == "etl_end":
            with postgres_hook.get_sqlalchemy_engine().connect() as connection:
                connection.execute(
                    """
                        UPDATE logs.dags_logs
                        SET time_end = %s, status = %s
                        WHERE dag_id = %s AND time_start = %s
                    """,
                    (current_time, "Успешно завершено", dag_id, start_time)
                )
            return

        try:
            with postgres_hook.get_sqlalchemy_engine().connect() as connection:
                connection.execute(
                    """
                        UPDATE logs.dags_logs
                        SET status = %s
                        WHERE dag_id = %s AND time_start = %s
                    """,
                    (f"В процессе - {task_id}", dag_id, start_time)
                )
            result = func(*args, **kwargs)
            return result
        except Exception as e:
            logging.error(e)
            with postgres_hook.get_sqlalchemy_engine().connect() as connection:
                connection.execute(
                    """
                        UPDATE logs.dags_logs
                        SET time_end = %s, status = %s, error_message = %s
                        WHERE dag_id = %s AND time_start = %s
                    """,
                    (current_time, f"Завершено с ошибкой в task_id - {task_id}", str(e), dag_id, start_time)
                )
            raise

    return wrapper