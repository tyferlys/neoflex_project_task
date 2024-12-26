import pandas as pd



def _convert_datetime(df: pd.DataFrame, date_key: str):
    formats = [
        "%Y-%m-%d",
        "%d-%m-%Y"
    ]

    try:
        df[date_key] = pd.to_datetime(df[date_key], errors="raise")
        return df[date_key].dt.strftime("%Y-%m-%d")
    except Exception as e:
        pass

    for format_item in formats:
        try:
            df[date_key] = pd.to_datetime(df[date_key], format=format_item, errors="raise")
            return df[date_key].dt.strftime("%Y-%m-%d")
        except Exception as e:
            continue



def transform_data_to_db(df: pd.DataFrame, metadata: dict):
    df.rename(columns={column: column.lower() for column in df.columns}, inplace=True)

    for not_null_key in metadata["list_not_null"]:
        df = df[df[not_null_key].notnull()]

    for key, value in metadata["list_length"].items():
        if key in df.columns:
            df = df[
                (df[key].isnull()) |
                ((df[key].str.len() >= value[0]) & (df[key].str.len() <= value[1]))
            ]

    for date_key in metadata["list_date"]:
        df[date_key] = _convert_datetime(df, date_key)

    if len(metadata["list_pk"]) > 0:
        df = df.drop_duplicates(subset=metadata["list_pk"])

    return df