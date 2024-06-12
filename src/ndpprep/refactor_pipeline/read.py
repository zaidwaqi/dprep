from io import StringIO
import pyarrow.parquet as pq
import pydoop.hdfs as hdfs
import tempfile
import pandas as pd


def read_parquet_file(file_path):
    tbl = pq.read_table(file_path)
    tbl = tbl.rename_columns([col.upper() for col in tbl.schema.names])
    return tbl


def read_parquet_file_with_temp(file_path):
    with tempfile.NamedTemporaryFile() as temp_file:
        with hdfs.open(file_path, "rb") as file:
            temp_file.write(file.read())

    tbl = pq.read_table(temp_file.name)
    tbl = tbl.rename_columns([col.upper() for col in tbl.schema.names])
    return tbl


def read_csv_file(file_path, seperator=",") -> pd.DataFrame:
    """
    This function return a pandas dataframe
    """
    # Read the text or CSV file from HDFS
    with hdfs.open(file_path, "rt", encoding="utf-8") as file:
        file.seek(0)  # Reset file pointer to the beginning
        csv_content = StringIO(file.read())  # Use StringIO to create a file-like object
        df = pd.read_csv(csv_content, header=None, sep=seperator, dtype=str)

    return df
