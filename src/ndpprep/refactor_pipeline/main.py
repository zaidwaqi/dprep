import time
import sys
from datetime import datetime

sys.path.append("/home/cdsw/Job Scripts/refactor_pipeline")
sys.path.append("/")
from ndpprep.refactor_pipeline.pipeline import Pipeline
from ndpprep.refactor_pipeline.util import (
    columns_cleanup,
    data_masking,
    durations_logging,
    encrypt_table,
    generate_output_file,
    list_parquet_csv_files,
    loop_from_csv,
    read_secret_key,
    save_to_sftp,
    update_target_schema,
    strip_and_replace,
)
from ndpprep.refactor_pipeline.read import read_parquet_file, read_parquet_file_with_temp
from ndpprep.refactor_pipeline.process import is_dataset_processed
from ndpprep.refactor_pipeline.common import (
    HIVE_PATH,
    SECRET_PATH,
    PROCESSED_FILE_PATH,
    TOTAL_PROCESSED_FILE_PATH,
)

EXTRACTION_DATE = "2024-05-19"
TARGET_USER = "ndp"
TARGET_IP = "172.20.100.213"
TARGET_PATH = "/data"
DATABASE_NAME = "daa_granite"
DATASET_NAME = "daa_granite"
JOB_ID = "granite_1"

# SECRET_KEY = read_secret_key(SECRET_PATH)
SECRET_KEY = "randomsecretkey"
FOLDER_PATH = f"{HIVE_PATH}/{DATABASE_NAME}/{DATASET_NAME}"


def log_dataset_duration(function):
    def wrapper(*args, **kwargs):
        try:
            start = time.time()
            function(*args, **kwargs)
            duration = time.time() - start
            hours, remainder = divmod(duration, 3600)
            minutes, seconds = divmod(remainder, 60)
            formatted_duration = "{:02}:{:02}:{:02}".format(
                int(hours), int(minutes), int(seconds)
            )
            current_timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

            with open(PROCESSED_FILE_PATH, "a") as file:
                file.write(
                    f"{EXTRACTION_DATE}/{DATABASE_NAME}/{DATASET_NAME} - Ingested successfully, Total duration: {formatted_duration}, Timestamp: {current_timestamp}, JobID: {JOB_ID}\n"
                )
        except FileNotFoundError:
            with open(PROCESSED_FILE_PATH, "a") as file:
                file.write(
                    f"{EXTRACTION_DATE}/{DATABASE_NAME}/{DATASET_NAME} - File not found, JobID: {JOB_ID}\n"
                )

    return wrapper


### Also, you can create custom read function
### in the pipeline, just replace "read_parquet_file" with custom_read_function
# def custom_read_function(file_path: str):
#     with hdfs.open(file_path, 'rt', encoding='utf-8') as file:
#         file.seek(0)  # Reset file pointer to the beginning
#         csv_content = StringIO(file.read())  # Use StringIO to create a file-like object
#         if DATASET_NAME == "daa_sftp_ntt_active_daily":
#             df = pd.read_csv(csv_content, sep='|', header=None, on_bad_lines='warn')
#         elif DATASET_NAME == "daa_http_speedometer_new_daily_v3":
#             df = pd.read_csv(csv_content, sep='~', header=None, on_bad_lines='warn')
#         else:
#             df = pd.read_csv(csv_content, header=None, dtype=str)
#     return df


@log_dataset_duration
def main(folder_path=FOLDER_PATH):
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
        # pipeline.add_step("read_function", read_parquet_file_with_temp)
        pipeline.add_step("columns_cleanup", columns_cleanup)
        # pipeline.add_step("columns_cleanup", strip_and_replace) # for csv
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


def cr_process(csv_file_path: str):
    df = loop_from_csv(csv_file_path, "cr")
    dataset_num = df.count()

    start = time.time()
    table_num = 0
    ingested_datasets = 0

    for _, row in df.iterrows():
        database_name = row["Database"]
        dataset_name = row["Table"]
        table_num = table_num + 1

        FOLDER_PATH = f"{HIVE_PATH}/{database_name}/{dataset_name}"

        main(FOLDER_PATH)

    duration = time.time() - start
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"Total duration: {duration}")
    print(f"Total dataset names ingested: {ingested_datasets}")
    with open(TOTAL_PROCESSED_FILE_PATH, "a") as file:
        file.write(
            f"{timestamp} Total dataset ingested in {JOB_ID} : {ingested_datasets}/{dataset_num}, Duration: {duration} \n"
        )


if __name__ == "__main__":
    print("Pipeline started ▶️")
    main()
    # cr_process("/home/cdsw/Job Scripts/dataset_in_scope/granite_1.txt")
