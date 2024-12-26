import logging

import pandas as pd

from airflow.providers.postgres.hooks.postgres import PostgresHook
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy import Table, MetaData


def load_data_to_db(df: pd.DataFrame, metadata: dict, is_append):
    postgres_hook: PostgresHook = PostgresHook("project-neoflex-db")
    engine = postgres_hook.get_sqlalchemy_engine()
    metadata_db = MetaData()
    table = Table(metadata["tablename"], metadata_db, schema=metadata["schema"], autoload_with=engine)

    with engine.connect() as connection:
        if not is_append:
            connection.execute(f"TRUNCATE TABLE {metadata["schema"]}.{metadata["tablename"]}")

        for start in range(0, len(df), 1000):
            batch = df.iloc[start:start + 1000].replace({pd.NA: None, pd.NaT: None, float('nan'): None}).to_dict(orient="records")

            insert_stmt = insert(table).values(batch)
            if len(metadata["list_pk"]) > 0:
                update_stmt = insert_stmt.on_conflict_do_update(
                    index_elements=metadata["list_pk"],
                    set_={key: getattr(insert_stmt.excluded, key) for key in df.columns if key not in metadata["list_pk"]}
                )
            else:
                update_stmt = insert_stmt

            connection.execute(update_stmt)
