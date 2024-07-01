import os
import base64

from azure.storage.filedatalake import DataLakeServiceClient
from azure.identity import ManagedIdentityCredential
import pyarrow.parquet as pq
from pyarrow._parquet_encryption import (
    CryptoFactory,
    KmsConnectionConfig,
    KmsClient
)

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
        master_key_bytes = self.master_keys_map[master_key_identifier].encode(
            'utf-8')
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
        if (expected_master_key == master_key_bytes.decode('utf-8')):
            return decrypted_key
        raise ValueError("Incorrect master key used",
                         master_key_bytes, decrypted_key)
def read_secret_key(secret_path:str):
    with open(secret_path, "rb") as f:
        secret_key = f.read()
    return secret_key
def decryption_props(secret_key:str):
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
    decryption_properties = crypto_factory.file_decryption_properties(kms_connection_config)

    return decryption_properties

def configuration():
    #==== Decrypt files
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
        credential=CREDENTIAL
    )
    file_system_client = service_client.get_file_system_client(file_system=AZURE_FILE_SYSTEM)

    return file_system_client, decryption_properties

# Main Functions
## - sftp_to_raw_with_db_name ✅
## - sftp_to_raw_with_db_name_dataset_name ✅
## - sftp_to_raw_with_db_name_extraction_date ✅
def sftp_to_raw_with_db_name(db_name:str):
    #==== Using Winscp, move from sftp to temp folder in ingestion VM
    script_template = r""" winscp.com /command ^
    "open sftp://ndp:Wd3vdm5fJE@172.20.100.213" ^
    "cd /data/{db_name}" ^
    "get -filemask={db_name}/* E:\NDP\test_01\{db_name}"
    exit
    """
    script_template = script_template.format(
        db_name=db_name,
    )
    # Run script as command
    # os.system(script_template)
    print("File downloaded successfully.")

    file_system_client, decryption_properties = configuration()

    ## List files in the target directory
    dataset_folders = os.listdir(f"E:\\NDP\\test_01\\{db_name}\\")
    for dataset_folder in dataset_folders:
        extraction_date_folders = os.listdir(f"E:\\NDP\\test_01\\{db_name}\\{dataset_folder}\\")
        for extraction_date_folder in extraction_date_folders:
            for filename in os.listdir(extraction_date_folder):
                file_path = os.path.join(extraction_date_folder, filename)
                with open(file_path, 'rb') as f:
                    table = pq.read_table(f, decryption_properties=decryption_properties)

                    # Print full column names with sample values
                    if not first_table_printed:
                        schema = table.schema
                        first_table_printed = False
                        for i, field in enumerate(schema):
                            column_name = field.name
                            column_value = table.column(column_name).to_pylist()[:3]
                            print(f"{column_name}: {column_value}")

                    try:
                        # Create folder if it doesn't exist
                        output_folder = f'datalake/raw/{db_name}/{dataset_folder}/{extraction_date_folder}'
                        file_system_client.create_directory(directory=output_folder)

                        with pq.write_table(table, file_path) as f:
                            # file_client = file_system_client.get_file_client(file_path="test.parquet")
                            file_client = file_system_client.create_file(f"{output_folder}/{filename}")
                            file_client.upload_data(f, overwrite=True)
                            print(f"File {filename} uploaded successfully.")

                        # If successful, delete the file in SFTP..? how?
                    except Exception as e:
                        print(f'Error: {e}, File {filename} not uploaded.')
def sftp_to_raw_with_db_name_dataset_name(db_name:str, dataset_name:str):
    #==== Using Winscp, move from sftp to temp folder in ingestion VM
    script_template = r"""winscp.com /command ^
    "open sftp://ndp:Wd3vdm5fJE@172.20.100.213" ^
    "cd /data/{db_name}/{dataset_name}" ^
    "get -filemask={db_name}/{dataset_name}/* E:\NDP\test_01\{db_name}\{dataset_name}" ^
    exit
    """
    script_template = script_template.format(
        db_name=db_name,
        dataset_name=dataset_name,
    )
    # Run script as command
    # os.system(script_template)
    print("File downloaded successfully.")

    file_system_client, decryption_properties = configuration()

    ## List files in the target directory
    extraction_date_folders = os.listdir(f"E:\\NDP\\test_01\\{db_name}\\{dataset_name}\\")
    for extraction_date_folder in extraction_date_folders:
        for filename in os.listdir(extraction_date_folder):
            file_path = os.path.join(extraction_date_folder, filename)
            with open(file_path, 'rb') as f:
                table = pq.read_table(f, decryption_properties=decryption_properties)

                # Print full column names with sample values
                if not first_table_printed:
                    schema = table.schema
                    first_table_printed = False
                    for i, field in enumerate(schema):
                        column_name = field.name
                        column_values = table[column_name].to_pylist()[:5]  # The first 5 values as sample
                        print(f"{column_name}: {column_values}")
                    first_table_printed = True

                try:
                    # Create folder if it doesn't exist
                    output_folder = f'datalake/raw/{db_name}/{dataset_name}/{extraction_date_folder}'
                    file_system_client.create_directory(directory=output_folder)

                    with pq.write_table(table, file_path) as f:
                        # file_client = file_system_client.get_file_client(file_path="test.parquet")
                        file_client = file_system_client.create_file(f"{output_folder}/{filename}")
                        file_client.upload_data(f, overwrite=True)
                        print(f"File {filename} uploaded successfully.")

                    # If successful, delete the file in SFTP..? how?
                except Exception as e:
                    print(f'Error: {e}, File {filename} not uploaded.')
def sftp_to_raw_with_db_name_extraction_date(db_name:str, extraction_date:str):
    #==== Using Winscp, move from sftp to temp folder in ingestion VM
    script_template = r"""winscp.com /command ^
    "open sftp://ndp:Wd3vdm5fJE@172.20.100.213" ^
    "cd /data/{db_name}" ^
    "get -filemask={db_name}/*/extraction_date={extraction_date}/* E:\NDP\test_01\{db_name}\"
    exit
    """
    script_template = script_template.format(
        db_name=db_name,
        extraction_date=extraction_date,
    )
    # Run script as command
    # os.system(script_template)
    print("File downloaded successfully.")

    file_system_client, decryption_properties = configuration()

    ## List files in the target directory
    dataset_folders = os.listdir(f"E:\\NDP\\test_01\\{db_name}\\")

    for dataset_folder in dataset_folders:
        extraction_date_folders = os.listdir(f"E:\\NDP\\test_01\\{db_name}\\{dataset_folder}\\")
        for extraction_date_folder in extraction_date_folders:
            for filename in os.listdir(extraction_date_folder):
                file_path = os.path.join(extraction_date_folder, filename)
                with open(file_path, 'rb') as f:
                    table = pq.read_table(f, decryption_properties=decryption_properties)

                    # Print full column names with sample values
                    if not first_table_printed:
                        schema = table.schema
                        first_table_printed = False
                        for i, field in enumerate(schema):
                            column_name = field.name
                            column_values = table[column_name].to_pylist()[:5]  # The first 5 values as sample
                            print(f"{column_name}: {column_values}")
                        first_table_printed = True

                    try:
                        # Create folder if it doesn't exist
                        output_folder = f'datalake/raw/{db_name}/{dataset_folder}/{extraction_date_folder}'
                        file_system_client.create_directory(directory=output_folder)

                        with pq.write_table(table, file_path) as f:
                            # file_client = file_system_client.get_file_client(file_path="test.parquet")
                            file_client = file_system_client.create_file(f"{output_folder}/{filename}")
                            file_client.upload_data(f, overwrite=True)
                            print(f"File {filename} uploaded successfully.")

                        # If successful, delete the file in SFTP..? how?
                    except Exception as e:
                        print(f'Error: {e}, File {filename} not uploaded.')

sftp_to_raw_with_db_name("db_test_01")
sftp_to_raw_with_db_name_extraction_date("db_test_01", "2022-01-01")
sftp_to_raw_with_db_name_dataset_name("db_test_01", "dataset_test_01")