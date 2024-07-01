import os

from ndpprep.refactor_pipeline.pipeline import Pipeline
from ndpprep.refactor_pipeline.main import log_dataset_duration
from ndpprep.refactor_pipeline.util import list_parquet_csv_files, list_parquet_csv_files_in_bulk
from ndpprep.refactor_pipeline.process import is_dataset_processed
from ndpprep.refactor_pipeline.read import read_parquet_file
from ndpprep.refactor_pipeline.util import (
    columns_cleanup,
    data_masking,
    durations_logging,
    encrypt_table,
    generate_output_file,
    list_parquet_csv_files,
    read_secret_key,
    save_to_sftp,
    update_target_schema,
)

@log_dataset_duration
def encrypt_to_sftp(folder_path:str, process_number:int, max_process_number:int):
    EXTRACTION_DATE = os.environ["EXTRACTION_DATE"]
    TARGET_USER = os.environ["TARGET_USER"]
    TARGET_IP = os.environ["TARGET_IP"]
    TARGET_PATH = os.environ["TARGET_PATH"]
    DATABASE_NAME = os.environ["DATABASE_NAME"]
    DATASET_NAME = os.environ["DATASET_NAME"]
    JOB_ID = os.environ["JOB_ID"]
    SECRET_PATH = os.environ["SECRET_PATH"]
    SECRET_KEY = read_secret_key(SECRET_PATH)

    if(process_number):
        parquet_csv_files = list_parquet_csv_files_in_bulk(folder_path, process_number, max_process_number)
    else:
        parquet_csv_files = list_parquet_csv_files(folder_path)

    for parquet_csv_file in parquet_csv_files:
        pipeline = Pipeline(
            extraction_date=EXTRACTION_DATE,
            target_user=TARGET_USER,
            target_ip=TARGET_IP,
            target_path=TARGET_PATH,
            database_name=DATABASE_NAME,
            dataset_name=DATASET_NAME,
            job_id=JOB_ID,
            secret_key=SECRET_KEY,
            file_path=parquet_csv_file,
        )
        pipeline.add_step(
            "is_dataset_processed",
            is_dataset_processed,
        )
        pipeline.add_step(
            "read_function",
            read_parquet_file,
        )
        pipeline.add_step(
            "columns_cleanup",
            columns_cleanup
        )
        pipeline.add_step(
            "update_target_schema",
            update_target_schema,
        )
        pipeline.add_step(
            "data_masking",
            data_masking,
        )
        pipeline.add_step(
            "generate_output_file",
            generate_output_file,
        )
        pipeline.add_step(
            "encrypt_table",
            encrypt_table,
        )
        pipeline.add_step(
            "save_to_sftp",
            save_to_sftp,
        )
        pipeline.add_step(
            "durations_logging",
            durations_logging,
        )
        pipeline.run()