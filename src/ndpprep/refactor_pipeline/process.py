import pydoop.hdfs as hdfs
import pyarrow as pa
import pyarrow.parquet as pq
import time
import os
from datetime import datetime
import sys

sys.path.append("/home/cdsw/Job Scripts/refactor_pipeline")
from ndpprep.refactor_pipeline.common import (
    SECRET_PATH,
    LOG_FILE_PATH,
    FAIL_PROCESSED_FILE_PATH,
    PROCESSED_FILE_PATH,
)
from ndpprep.refactor_pipeline.util import (
    read_secret_key,
    columns_cleanup,
    update_target_schema,
    data_masking,
    generate_output_file,
    encrypt_table,
    save_to_sftp,
    durations_logging,
)


def is_dataset_processed(
    extraction_date: str,
    database_name: str,
    dataset_name: str,
    processed_file_path: str,
) -> bool:
    processed_entries = set()
    if os.path.exists(processed_file_path):
        with open(processed_file_path, "r") as log_file:
            for line in log_file:
                if line.strip():
                    processed_entries.add(line.strip())
    return (
        f"{extraction_date}/{database_name}/{dataset_name} - Ingested successfully\n"
        in processed_entries
    )


def process_dataset(
    database_name: str,
    dataset_name: str,
    extraction_date: str,
    job_id: str,
    target_user: str,
    target_ip: str,
    target_path: str,
    parquet_csv_files,
    read_function,
    **kwargs,
) -> None:
    secret_key = read_secret_key(SECRET_PATH)

    for parquet_csv_file in parquet_csv_files:
        # if not parquet_file.endswith(".parquet"):
        #     continue
        if extraction_date not in parquet_csv_file:
            return
        # Process the parquet file
        try:
            read_start = time.time()
            with hdfs.open(parquet_csv_file) as f:
                # Read Table
                if "seperator" in kwargs:
                    tbl_or_df = read_function(f, kwargs["seperator"])
                else:
                    tbl_or_df = read_function(f)
                read_duration = time.time() - read_start
                # Columns Cleanup
                tbl_or_df = columns_cleanup(tbl_or_df)
                # Update Target Schema
                tbl = update_target_schema(tbl_or_df, dataset_name)
                # Data Masking
                tbl, masked_duration, column_masked_count = data_masking(
                    tbl, job_id=job_id, database_name=database_name
                )
                # Generate Output File
                output_file = generate_output_file(
                    tbl, database_name, dataset_name, extraction_date
                )
                # Encrypt Table
                encrypt_duration = encrypt_table(tbl, output_file, secret_key)
                parquet_memory_size_mb = tbl.nbytes / (1024 * 1024)
                # Save to SFTP
                sftp_upload_duration = save_to_sftp(
                    output_file, target_user, target_ip, target_path
                )
                # Durations Logging
                durations_logging(
                    log_file_path=LOG_FILE_PATH,
                    today_date=str(extraction_date).replace("-", ""),
                    file_name=parquet_csv_file,
                    read_duration=read_duration,
                    masked_duration=masked_duration,
                    encrypt_duration=encrypt_duration,
                    sftp_upload_duration=sftp_upload_duration,
                    column_masked_count=column_masked_count,
                    memory_size_mb=parquet_memory_size_mb,
                )

        except ValueError as e:
            print(
                f"ValueError encountered while processing file '{parquet_csv_file}': {e}"
            )
            timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            with open(FAIL_PROCESSED_FILE_PATH, "a") as fail:
                fail.write(
                    f"{timestamp} - ValueError processing dataset '{dataset_name}' in database '{database_name}': {e}\n"
                )
            # Move to the next dataset
            return  # proceed to the next dataset
        except pa.ArrowInvalid as e:
            print(f"Error while processing file '{parquet_csv_file}': {e}")
            with open(PROCESSED_FILE_PATH, "a") as fail:
                fail.write(
                    f"{extraction_date}/{database_name}/{dataset_name} - Error while processing data\n"
                )
            # Move to the next dataset
            return  # proceed to the next dataset
        print(f"File found for extraction date '{extraction_date}' in '{dataset_name}'")


def check_and_process(
    database_name,
    dataset_name,
    extraction_date,
    job_id,
    target_user,
    target_ip,
    target_path,
    parquet_csv_files,
    read_function,
    **kwargs,
):
    if not is_dataset_processed(
        extraction_date, database_name, dataset_name, PROCESSED_FILE_PATH
    ):
        start_ds = time.time()
        try:
            process_dataset(
                database_name,
                dataset_name,
                extraction_date,
                job_id,
                target_user,
                target_ip,
                target_path,
                parquet_csv_files,
                read_function,
                **kwargs,
            )

            ds_duration = time.time() - start_ds
            hours, remainder = divmod(ds_duration, 3600)
            minutes, seconds = divmod(remainder, 60)
            formatted_duration = "{:02}:{:02}:{:02}".format(
                int(hours), int(minutes), int(seconds)
            )
            current_timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

            with open(PROCESSED_FILE_PATH, "a") as file:
                file.write(
                    f"{extraction_date}/{database_name}/{dataset_name} - Ingested successfully, Total duration: {formatted_duration}, Timestamp: {current_timestamp}, JobID: {job_id}\n"
                )

            return True
        except FileNotFoundError:
            with open(PROCESSED_FILE_PATH, "a") as file:
                file.write(
                    f"{extraction_date}/{database_name}/{dataset_name} - File not found, JobID: {job_id}\n"
                )
            return False
    else:
        print(
            f"Skipping {extraction_date}/{database_name}/{dataset_name} as it is already processed"
        )
        return False
