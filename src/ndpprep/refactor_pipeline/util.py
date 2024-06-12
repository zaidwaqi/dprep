from io import StringIO
import pyarrow as pa
import pyarrow.parquet as pq
import pyarrow.parquet.encryption as pe
from pyarrow.tests.parquet.encryption import InMemoryKmsClient

import time
import uuid
import os
import pandas as pd
import numpy as np
import paramiko
import pydoop.hdfs as hdfs


def read_secret_key(secret_path):
    with open(secret_path, "rb") as f:
        secret_key = f.read()
    return secret_key


def masked_uuid(data):
    # Generate a UUID based on the input data
    randno = "1x7h3"
    uuid_val = str(uuid.uuid5(uuid.NAMESPACE_X500, randno + str(data)))
    # Take the first 8 characters of the UUID and remove hyphens
    shortened_uuid = uuid_val[:12].replace("-", "")
    return shortened_uuid


def encrypt_table(tbl, output_file, secret_key):
    encrypt_start = time.time()
    # Encryption configuration using the processed table tbl and its columns
    encryption_config = pe.EncryptionConfiguration(
        footer_key="footer",
        column_keys={
            "columns": tbl.schema.names,
        },
        encryption_algorithm="AES_GCM_V1",
        data_key_length_bits=128,
    )

    # KMS connection configuration
    kms_connection_config = pe.KmsConnectionConfig(
        custom_kms_conf={
            "footer": secret_key.decode("UTF-8"),
            "columns": secret_key.decode("UTF-8"),
        }
    )

    # Crypto factory
    def kms_factory(kms_connection_configuration):
        return InMemoryKmsClient(kms_connection_configuration)

    crypto_factory = pe.CryptoFactory(kms_factory)

    # Encrypt the table
    encryption_properties = crypto_factory.file_encryption_properties(
        kms_connection_config, encryption_config
    )

    # Write encrypted PyArrow Table to a Parquet file
    with pq.ParquetWriter(
        output_file, tbl.schema, encryption_properties=encryption_properties
    ) as writer:
        writer.write_table(tbl)

    encrypt_duration = time.time() - encrypt_start

    return encrypt_duration


def save_to_sftp(output_file, target_user, target_ip, target_path):
    try:
        sftp_start = time.time()
        ssh = paramiko.SSHClient()
        ssh.load_system_host_keys()
        ssh.connect(target_ip, username=target_user)

        sftp = ssh.open_sftp()

        # Split the output_file path to get the directory structure
        directory_structure = os.path.dirname(output_file)

        # Check if directory_structure exists on SFTP, if not, create it
        try:
            sftp.chdir(target_path)
        except IOError:
            sftp.mkdir(target_path)
            sftp.chdir(target_path)

        # Iterate over the directory structure and create folders on SFTP
        for folder in directory_structure.split("/"):
            try:
                sftp.chdir(folder)
            except IOError:
                sftp.mkdir(folder)
                sftp.chdir(folder)

        # Upload the Parquet file to SFTP
        sftp.put(output_file, os.path.join(target_path, output_file))

        print(f"File '{output_file}' uploaded to SFTP successfully.")

        sftp.close()
        ssh.close()

        sftp_upload_duration = time.time() - sftp_start
        return sftp_upload_duration

    except Exception as e:
        print(f"Error occurred while uploading file to SFTP: {e}")


def loop_from_csv(file_path: str, process_type: str):
    if process_type == "cr":
        with open(file_path, "rt", encoding="utf-8") as f:
            f.seek(0)
            csv_content = StringIO(f.read())
            df = pd.read_csv(csv_content)
    else:
        with open(file_path, "r") as file:
            lines = file.readlines()
        # Split each line by '/' and store the results
        data = [line.strip().split("/") for line in lines]
        # Convert the data into a DataFrame
        df = pd.DataFrame(data, columns=["Database", "Table"])

    return df


def list_parquet_csv_files(path):
    paths = hdfs.ls(path, recursive=True)
    filtered_data = [file for file in paths if hdfs.path.isfile(file)]
    return filtered_data


def strip_and_replace(df):  # for csv
    # Strip whitespace from each element
    df = df.apply(lambda col: col.str.strip() if col.dtype == "object" else col)
    # Replace empty strings and '-' with None
    df.replace({"": np.nan, "-": np.nan, "None": np.nan}, inplace=True)
    return df


def columns_cleanup(tbl_or_df):
    if tbl_or_df is pd.DataFrame:
        df = strip_and_replace(tbl_or_df)
        return df
    else:
        # Strip whitespace from data in each column and replace empty strings with None
        for i in range(tbl_or_df.num_columns):
            col = tbl_or_df.column(i)
            if pa.types.is_string(col.type):
                # Apply strip function to each chunk within the column
                stripped_chunks = [
                    pa.array(
                        chunk.to_pandas()
                        .str.strip()
                        .replace({"": None, "-": None, "None": None}),
                        type=col.type,
                    )
                    if chunk is not None
                    else None
                    for chunk in col.iterchunks()
                    # for chunk in col.data.chunks
                ]
                # Reconstruct the column with stripped chunks
                stripped_col = pa.chunked_array(stripped_chunks)
                # Replace the column in the table with the stripped column
                columns = [
                    stripped_col if j == i else col
                    for j, col in enumerate(tbl_or_df.columns)
                ]
                tbl = pa.table(columns, schema=tbl_or_df.schema)

        return tbl


def update_target_schema(tbl, dataset_name):
    target_schema_function_name = f"get_target_schema_{dataset_name}"

    if target_schema_function_name in globals():
        target_schema_function = globals()[target_schema_function_name]
        target_schema = target_schema_function(tbl.schema.names)
        tbl = tbl.cast(target_schema)

    return tbl


def update_mapping(original_columns, masked_columns, job_id, dbname):
    try:
        data_dict = {"ORIGINAL_VALUE": [], "MASKED_VALUE": []}
        existing_combinations = set()

        for original_column, masked_column in zip(original_columns, masked_columns):
            for original_val, masked_val in zip(original_column, masked_column):
                # Check if the combination already exists
                if (original_val, masked_val) not in existing_combinations:
                    data_dict["ORIGINAL_VALUE"].append(original_val)
                    data_dict["MASKED_VALUE"].append(masked_val)
                    existing_combinations.add((original_val, masked_val))

        df = pd.DataFrame(data_dict)

        # Define the folder path based on dbname
        mapping_table_folder = f"/home/cdsw/Job Scripts/mapping_table/{dbname}"
        os.makedirs(mapping_table_folder, exist_ok=True)

        output_file_path = f"{mapping_table_folder}/mapping_table_{job_id}.parquet"
        if os.path.exists(output_file_path):
            existing_df = pd.read_parquet(output_file_path)
            existing_combinations = set(
                zip(existing_df["ORIGINAL_VALUE"], existing_df["MASKED_VALUE"])
            )
            # Find and remove duplicates
            df_no_duplicates = (
                pd.concat([existing_df, df])
                .drop_duplicates(subset=["MASKED_VALUE"])
                .reset_index(drop=True)
            )
        else:
            # If the file doesn't exist, use the original dataframe
            df_no_duplicates = df

        df_no_duplicates.to_parquet(output_file_path, index=False)

        print(f"mapping_table_{job_id}.parquet updated successfully.")

    except Exception as e:
        print(f"Error while updating mapping_table_{job_id}.parquet: {e}")


def data_masking(tbl, job_id, database_name):
    modified_columns = []
    masked_start = time.time()
    column_masked_count = 0
    masked_columns = []
    original_columns = []

    for col_name in tbl.schema.names:
        if (
            tbl.schema.field(col_name).metadata
            and b"PII" in tbl.schema.field(col_name).metadata
        ):
            print(f"Column '{col_name}' metadata indicates PII:")
            print(
                f"Masking values in column '{col_name}' with metadata: {tbl.schema.field(col_name).metadata}"
            )
            original_columns.append(tbl[col_name])
            column_masked_count += 1
            # Masking with 'XXX', but only if the value is not null
            masked_column = [
                "XXX" if val is not None and not np.isnan(val) else None
                for val in tbl[col_name]
            ]

            masked_columns.append(masked_column)
            modified_columns.append(masked_column)
        elif (
            tbl.schema.field(col_name).metadata
            and tbl.schema.field(col_name).metadata.get(b"req") == b"sensitive"
        ):
            print(f"Column '{col_name}' metadata:")
            print(
                f"(MASKED) Processing sensitive column '{col_name}' with metadata: {tbl.schema.field(col_name).metadata}"
            )
            original_columns.append(tbl[col_name])
            column_masked_count += 1
            modified_column = [
                masked_uuid(str(val)) if val is not None and not np.isnan(val) else None
                for val in tbl[col_name]
            ]
            masked_columns.append(modified_column)
            modified_columns.append(modified_column)
        else:
            modified_column = tbl[col_name]
            modified_columns.append(modified_column)

    # Generating mapping table, please add in .gitignore
    # Feels weird to do it here
    update_mapping(original_columns, masked_columns, job_id, database_name)

    tbl = pa.table(modified_columns, schema=tbl.schema)
    masked_duration = time.time() - masked_start

    return tbl, masked_duration, column_masked_count


def generate_output_file(parquet_file, database_name, dataset_name, extraction_date):
    output_folder = f"{database_name}/{dataset_name}/extraction_date={extraction_date}"
    os.makedirs(output_folder, exist_ok=True)
    output_file = os.path.join(output_folder, os.path.basename(parquet_file))

    return output_file


def durations_logging(
    log_file_path,
    today_date,
    file_name,
    read_duration,
    masked_duration,
    encrypt_duration,
    sftp_upload_duration,
    column_masked_count,
    memory_size_mb,
):
    with open(log_file_path, "a") as log_file:
        log_file.write(
            f"{today_date}_{file_name}: read_duration: {read_duration:.2f} seconds, masked_duration: {masked_duration:.2f} seconds, encrypt_duration: {encrypt_duration:.2f} seconds, upload_to_sftp_duration: {sftp_upload_duration:.2f} seconds, count masked column: {column_masked_count}, memory size (mb): {memory_size_mb}\n"
        )
