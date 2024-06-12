import time
import sys
from datetime import datetime
import pyarrow as pa

sys.path.append("/home/cdsw/Job Scripts/refactor_pipeline")
from ndpprep.refactor_pipeline.common import (
    LOG_FILE_PATH,
    PROCESSED_FILE_PATH,
    FAIL_PROCESSED_FILE_PATH,
)


class Pipeline:
    def __init__(
        self,
        extraction_date,
        target_user,
        target_ip,
        target_path,
        database_name,
        dataset_name,
        job_id,
        secret_key,
        file_path,
    ):
        self.extraction_date = extraction_date
        self.database_name = database_name
        self.dataset_name = dataset_name
        self.job_id = job_id
        self.target_user = target_user
        self.target_ip = target_ip
        self.target_path = target_path
        self.secret_key = secret_key
        self.file_path = file_path

        self.tbl = None
        self.steps_names = []
        self.steps = []
        self.steps_params = []

    def add_step(self, step_name, step, **kwargs):
        self.steps_names.append(step_name)
        self.steps.append(step)
        self.steps_params.append(kwargs)

    def run(self):
        tbl = None
        read_duration = None
        output_file_path = None
        masked_duration = None
        column_masked_count = None
        encrypt_duration = None
        try:
            for step_name, step, params in zip(
                self.steps_names, self.steps, self.steps_params
            ):
                if step_name == "is_dataset_processed":
                    result = step(
                        extraction_date=self.extraction_date,
                        database_name=self.database_name,
                        dataset_name=self.dataset_name,
                        processed_file_path=PROCESSED_FILE_PATH,
                        **params,
                    )
                    if result:
                        print(
                            f"Skipping {self.extraction_date}/{self.database_name}/{self.dataset_name} as it is already processed"
                        )
                        break
                if step_name == "read_function":
                    read_start = time.time()
                    tbl = step(file_path=self.file_path, **params)
                    read_duration = time.time() - read_start
                if step_name == "columns_cleanup":
                    tbl = step(tbl=tbl)
                if step_name == "update_target_schema":
                    tbl = step(tbl=tbl, dataset_name=self.dataset_name, **params)
                if step_name == "data_masking":
                    tbl, masked_duration, column_masked_count = step(
                        tbl=tbl,
                        job_id=self.job_id,
                        database_name=self.database_name,
                        **params,
                    )
                if step_name == "generate_output_file":
                    output_file_path = step(
                        tbl=tbl,
                        database_name=self.database_name,
                        dataset_name=self.dataset_name,
                        extraction_date=self.extraction_date,
                        **params,
                    )
                if step_name == "encrypt_table":
                    encrypt_duration = step(
                        tbl=tbl,
                        output_file=output_file_path,
                        secret_key=self.secret_key,
                        **params,
                    )
                    parquet_memory_size_mb = tbl.nbytes / (1024 * 1024)
                if step_name == "save_to_sftp":
                    sftp_upload_duration = step(
                        target_user=self.target_user,
                        target_ip=self.target_ip,
                        target_path=self.target_path,
                        **params,
                    )
                if step_name == "durations_logging":
                    step(
                        log_file_path=LOG_FILE_PATH,
                        today_date=str(self.extraction_date).replace("-", ""),
                        file_name=self.file_path,
                        read_duration=read_duration,
                        masked_duration=masked_duration,
                        encrypt_duration=encrypt_duration,
                        sftp_upload_duration=sftp_upload_duration,
                        column_masked_count=column_masked_count,
                        memory_size_mb=parquet_memory_size_mb,
                        **params,
                    )

            print(
                f"File found for extraction date '{self.extraction_date}' in '{self.dataset_name}'"
            )
        except ValueError as e:
            print(
                f"ValueError encountered while processing file '{self.parquet_csv_file}': {e}"
            )
            timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            with open(FAIL_PROCESSED_FILE_PATH, "a") as fail:
                fail.write(
                    f"{timestamp} - ValueError processing dataset '{self.dataset_name}' in database '{self.database_name}': {e}\n"
                )
            # Move to the next dataset
            return  # proceed to the next dataset
        except pa.ArrowInvalid as e:
            print(f"Error while processing file '{self.parquet_csv_file}': {e}")
            with open(PROCESSED_FILE_PATH, "a") as fail:
                fail.write(
                    f"{self.extraction_date}/{self.database_name}/{self.dataset_name} - Error while processing data\n"
                )
            # Move to the next dataset
            return  # proceed to the next dataset
