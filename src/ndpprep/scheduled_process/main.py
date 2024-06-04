from datetime import datetime
import time

from process import check_and_process
from util import loop_from_csv, list_parquet_csv_files
import read as rd
from common import TOTAL_PROCESSED_FILE_PATH

EXTRACTION_DATE = "2024-05-19"
TARGET_USER = "ndp"
TARGET_IP = "172.20.100.213"
TARGET_PATH = "/data"
JOB_ID = "granite_1"
DATABASE_NAME = "daa_granite"
DATASET_NAME = "daa_granite"
CSV_OR_PARQUET = "parquet"
HIVE_PATH = "hdfs://nerveprdha/warehouse/tablespace/external/hive"

### Also, you can create custom read function
### in check_and_process, just replace read_function with custom_read_function
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


def main():
    FOLDER_PATH = f"{HIVE_PATH}/{DATABASE_NAME}/{DATASET_NAME}"
    parquet_csv_files = list_parquet_csv_files(FOLDER_PATH)
    check_and_process(
        DATABASE_NAME,
        DATASET_NAME,
        EXTRACTION_DATE,
        JOB_ID,
        TARGET_USER,
        TARGET_IP,
        TARGET_PATH,
        parquet_csv_files,
        read_function=rd.read_parquet_file,
        # seperator=",",
    )


def backdate(csv_file_path: str):
    df = loop_from_csv(csv_file_path)
    dataset_num = df.count()

    start = time.time()
    table_num = 0
    ingested_datasets = 0

    for _, row in df.iterrows():
        database_name = row["Database"]
        dataset_name = row["Table"]
        table_num = table_num + 1

        FOLDER_PATH = f"{HIVE_PATH}/{dataset_name}/{dataset_name}"
        parquet_csv_files = list_parquet_csv_files(FOLDER_PATH)

        if check_and_process(
            database_name,
            dataset_name,
            EXTRACTION_DATE,
            JOB_ID,
            TARGET_USER,
            TARGET_IP,
            TARGET_PATH,
            parquet_csv_files,
            read_function=rd.read_parquet_file,
        ):
            ingested_datasets += 1

    duration = time.time() - start
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"Total duration: {duration}")
    print(f"Total dataset names ingested: {ingested_datasets}")
    with open(TOTAL_PROCESSED_FILE_PATH, "a") as file:
        file.write(
            f"{timestamp} Total dataset ingested in {JOB_ID} : {ingested_datasets}/{dataset_num}, Duration: {duration} \n"
        )


if __name__ == "__main__":
    main()
    # backdate("/home/cdsw/Job Scripts/dataset_in_scope/granite_1.txt")
