import io
import os
import base64
import subprocess

from azure.storage.filedatalake import DataLakeServiceClient
from azure.identity import ManagedIdentityCredential, DefaultAzureCredential
import pyarrow.parquet as pq
import pyarrow as pa
from pyarrow._parquet_encryption import CryptoFactory, KmsConnectionConfig, KmsClient


SFTP_USERNAME = os.environ["SFTP_USERNAME"]
SFTP_PASSWORD = os.environ["SFTP_PASSWORD"]
SFTP_HOST = os.environ["SFTP_HOST"]


# Utility Functions
class InMemoryKmsClient(KmsClient):
    """This is a mock class implementation of KmsClient, built for testing
    only.
    """

    def __init__(self, config):
        """Create an InMemoryKmsClient instance."""
        KmsClient.__init__(self)
        self.master_keys_map = config.custom_kms_conf

    def wrap_key(self, key_bytes, master_key_identifier):
        """Not a secure cipher - the wrapped key
        is just the master key concatenated with key bytes"""
        master_key_bytes = self.master_keys_map[master_key_identifier].encode("utf-8")
        wrapped_key = b"".join([master_key_bytes, key_bytes])
        result = base64.b64encode(wrapped_key)
        return result

    def unwrap_key(self, wrapped_key, master_key_identifier):
        """Not a secure cipher - just extract the key from
        the wrapped key"""
        expected_master_key = self.master_keys_map[master_key_identifier]
        decoded_wrapped_key = base64.b64decode(wrapped_key)
        master_key_bytes = decoded_wrapped_key[:16]
        decrypted_key = decoded_wrapped_key[16:]
        if expected_master_key == master_key_bytes.decode("utf-8"):
            return decrypted_key
        raise ValueError("Incorrect master key used", master_key_bytes, decrypted_key)


def read_secret_key(secret_path: str):
    with open(secret_path, "rb") as f:
        secret_key = f.read()
    return secret_key


def decryption_props(secret_key: str):
    # KMS connection configuration
    kms_connection_config = KmsConnectionConfig(
        custom_kms_conf={
            "footer": secret_key.decode("UTF-8"),
            "columns": secret_key.decode("UTF-8"),
        }
    )

    # Crypto factory
    def kms_factory(kms_connection_configuration):
        return InMemoryKmsClient(kms_connection_configuration)

    crypto_factory = CryptoFactory(kms_factory)

    # Decryption properties
    decryption_properties = crypto_factory.file_decryption_properties(
        kms_connection_config
    )

    return decryption_properties


def configuration():
    # ==== Decrypt files
    SECRET_PATH = os.environ["SECRET_PATH"]
    SECRET_KEY = read_secret_key(SECRET_PATH)
    decryption_properties = decryption_props(SECRET_KEY)

    # Data Configuration
    AZURE_STORAGE_ACCOUNT = os.environ["AZURE_STORAGE_ACCOUNT"]
    AZURE_FILE_SYSTEM = os.environ["AZURE_FILE_SYSTEM"]

    # Create credentials
    CREDENTIAL = ManagedIdentityCredential()
    service_client = DataLakeServiceClient(
        account_url=f"https://{AZURE_STORAGE_ACCOUNT}.dfs.core.windows.net",
        credential=CREDENTIAL,
    )
    file_system_client = service_client.get_file_system_client(
        file_system=AZURE_FILE_SYSTEM
    )
    # file_systems = service_client.list_file_systems()
    # for fs in file_systems:
    #     print(fs.name)

    return file_system_client, decryption_properties


# Main Functions
## - sftp_to_raw_with_db_name ✅
## - sftp_to_raw_with_db_name_dataset_name ✅
## - sftp_to_raw_with_db_name_extraction_date ✅
def sftp_to_raw_with_db_name(db_name: str):
    try:
        # ==== Using Winscp, move from sftp to temp folder in ingestion VM
        script_template = r"""winscp.com /command ^
        "open sftp://{sftp_username}:{sftp_password}@{sftp_host}/" ^
        "cd /data/{db_name}" ^
        "get -filemask=* * E:\NDP\DECRYPT\{db_name}\" ^
        "exit"
        """
        script_template = script_template.format(
            sftp_username=SFTP_USERNAME,
            sftp_password=SFTP_PASSWORD,
            sftp_host=SFTP_HOST,
            db_name=db_name,
        )
        batch_file_path = "script.bat"
        with open(batch_file_path, "w") as file:
            file.write(script_template)

        subprocess.run(batch_file_path, shell=True)
        print("File downloaded successfully.")

    except Exception as e:
        print(f"Error occurred: {e}, File not downloaded successfully.")
        return

    file_system_client, decryption_properties = configuration()

    ## List files in the target directory
    print("Storing into Azure Data Lake..")
    dataset_folders = os.listdir(f"E:\\NDP\\DECRYPT\\{db_name}\\")
    for dataset_folder in dataset_folders:
        extraction_date_folders = os.listdir(
            f"E:\\NDP\\DECRYPT\\{db_name}\\{dataset_folder}\\"
        )
        for extraction_date_folder in extraction_date_folders:
            filenames = os.listdir(
                f"E:\\NDP\\DECRYPT\\{db_name}\\{dataset_folder}\\{extraction_date_folder}\\"
            )
            for filename in filenames:
                first_table_printed = False
                file_path = os.path.join(
                    f"E:\\NDP\\DECRYPT\\{db_name}\\{dataset_folder}\\{extraction_date_folder}\\",
                    filename,
                )
                with open(file_path, "rb") as f:
                    encrypted_parquet_file = pq.ParquetFile(
                        f, decryption_properties=decryption_properties
                    )
                    table = encrypted_parquet_file.read()
                    # Print full column names with sample values
                    if not first_table_printed:
                        schema = table.schema
                        first_table_printed = False
                        for i, field in enumerate(schema):
                            column_name = field.name
                            column_value = table[column_name].to_pylist()[:3]
                            print(f"{column_name}: {column_value}")
                        first_table_printed = True
                    try:
                        # Create folder if it doesn't exist
                        output_folder = (
                            f"{db_name}/{dataset_folder}/{extraction_date_folder}"
                        )
                        file_system_client.create_directory(directory=output_folder)

                        with file_system_client.get_file_client(
                            file_path=f"{output_folder}/{filename}"
                        ) as file_client:
                            file_client.create_file()

                            parquet_bytes = io.BytesIO()
                            pq.write_table(table, parquet_bytes)
                            file_client.upload_data(
                                parquet_bytes.getvalue(), overwrite=True
                            )

                            file_client.flush_data(len(parquet_bytes.getvalue()))

                        print(f"File {filename} has successfully uploaded.")

                        # If successful, delete the file in SFTP..? how?
                    except Exception as e:
                        print(f"Error: {e}, File {filename} not uploaded.")


def sftp_to_raw_with_db_name_dataset_name(db_name: str, dataset_name: str):
    try:
        # ==== Using Winscp, move from sftp to temp folder in ingestion VM
        script_template = r"""winscp.com /command ^
        "open sftp://{sftp_username}:{sftp_password}@{sftp_host}/" ^
        "cd /data/{db_name}" ^
        "get -filemask={db_name}/{dataset_name}/* * E:\NDP\DECRYPT\{db_name}\{dataset_name}\\" ^
        "exit"
        """
        script_template = script_template.format(
            sftp_username=SFTP_USERNAME,
            sftp_password=SFTP_PASSWORD,
            sftp_host=SFTP_HOST,
            db_name=db_name,
            dataset_name=dataset_name,
        )
        batch_file_path = "script.bat"
        with open(batch_file_path, "w") as file:
            file.write(script_template)

        subprocess.run(batch_file_path, shell=True)
        print("File downloaded successfully.")

    except Exception as e:
        print(f"Error occurred: {e}, File not downloaded successfully.")
        return

    file_system_client, decryption_properties = configuration()

    ## List files in the target directory
    extraction_date_folders = os.listdir(
        f"E:\\NDP\\DECRYPT\\{db_name}\\{dataset_name}\\"
    )
    for extraction_date_folder in extraction_date_folders:
        filenames = os.listdir(
            f"E:\\NDP\\DECRYPT\\{db_name}\\{dataset_name}\\{extraction_date_folder}\\"
        )
        for filename in filenames:
            first_table_printed = False
            file_path = os.path.join(
                f"E:\\NDP\\DECRYPT\\{db_name}\\{dataset_name}\\{extraction_date_folder}\\",
                filename,
            )
            with open(file_path, "rb") as f:
                encrypted_parquet_file = pq.ParquetFile(
                    f, decryption_properties=decryption_properties
                )
                table = encrypted_parquet_file.read()
                # Print full column names with sample values
                if not first_table_printed:
                    schema = table.schema
                    first_table_printed = False
                    for i, field in enumerate(schema):
                        column_name = field.name
                        column_value = table[column_name].to_pylist()[:3]
                        print(f"{column_name}: {column_value}")
                    first_table_printed = True
                try:
                    # Create folder if it doesn't exist
                    output_folder = f"{db_name}/{dataset_name}/{extraction_date_folder}"
                    file_system_client.create_directory(directory=output_folder)

                    with file_system_client.get_file_client(
                        file_path=f"{output_folder}/{filename}"
                    ) as file_client:
                        file_client.create_file()

                        parquet_bytes = io.BytesIO()
                        pq.write_table(table, parquet_bytes)
                        file_client.upload_data(
                            parquet_bytes.getvalue(), overwrite=True
                        )

                        file_client.flush_data(len(parquet_bytes.getvalue()))

                    print(f"File {filename} has successfully uploaded.")

                    # If successful, delete the file in SFTP..? how?
                except Exception as e:
                    print(f"Error: {e}, File {filename} not uploaded.")


def sftp_to_raw_with_db_name_extraction_date(db_name: str, extraction_date: str):
    try:
        # ==== Using Winscp, move from sftp to temp folder in ingestion VM
        script_template = r"""winscp.com /command ^
        "open sftp://{sftp_username}:{sftp_password}@{sftp_host}/" ^
        "cd /data/{db_name}" ^
        "get -filemask={db_name}/*/extraction_date={extraction_date}/* * E:\NDP\DECRYPT\{db_name}\\" ^
        "exit"
        """
        script_template = script_template.format(
            sftp_username=SFTP_USERNAME,
            sftp_password=SFTP_PASSWORD,
            sftp_host=SFTP_HOST,
            db_name=db_name,
            extraction_date=extraction_date,
        )
        batch_file_path = "script.bat"
        with open(batch_file_path, "w") as file:
            file.write(script_template)

        subprocess.run(batch_file_path, shell=True)
        print("File downloaded successfully.")

    except Exception as e:
        print(f"Error occurred: {e}, File not downloaded successfully.")
        return

    file_system_client, decryption_properties = configuration()

    ## List files in the target directory
    dataset_folders = os.listdir(f"E:\\NDP\\DECRYPT\\{db_name}\\")

    for dataset_folder in dataset_folders:
        extraction_date_folders = os.listdir(
            f"E:\\NDP\\DECRYPT\\{db_name}\\{dataset_folder}\\"
        )
        for extraction_date_folder in extraction_date_folders:
            extraction_date_folder_path = os.path.join(
                f"E:\\NDP\\DECRYPT\\{db_name}\\{dataset_folder}\\",
                extraction_date_folder,
            )
            filenames = os.listdir(extraction_date_folder_path)
            for filename in filenames:
                first_table_printed = False
                file_path = os.path.join(extraction_date_folder_path, filename)
                with open(file_path, "rb") as f:
                    table = pq.read_table(
                        f, decryption_properties=decryption_properties
                    )

                    # Print full column names with sample values
                    if not first_table_printed:
                        schema = table.schema
                        first_table_printed = False
                        for i, field in enumerate(schema):
                            column_name = field.name
                            column_values = table[column_name].to_pylist()[
                                :5
                            ]  # The first 5 values as sample
                            print(f"{column_name}: {column_values}")
                        first_table_printed = True

                    try:
                        # Create folder if it doesn't exist
                        output_folder = (
                            f"{db_name}/{dataset_folder}/{extraction_date_folder}"
                        )
                        file_system_client.create_directory(directory=output_folder)

                        with pq.write_table(table, file_path) as f:
                            # file_client = file_system_client.get_file_client(file_path="test.parquet")
                            file_client = file_system_client.create_file(
                                f"{output_folder}/{filename}"
                            )
                            file_client.upload_data(f, overwrite=True)
                            print(f"File {filename} uploaded successfully.")

                        # If successful, delete the file in SFTP..? how?
                    except Exception as e:
                        print(f"Error: {e}, File {filename} not uploaded.")


sftp_to_raw_with_db_name("daa_next.db")
# sftp_to_raw_with_db_name_extraction_date("db_test_01", "2022-01-01")
# sftp_to_raw_with_db_name_dataset_name("db_test_01", "dataset_test_01")
