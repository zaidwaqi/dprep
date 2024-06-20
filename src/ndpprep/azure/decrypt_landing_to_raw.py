import os
import pyarrow.parquet as pq

from ndpprep.encryption.main import InMemoryKmsClient, pe

from azure.storage.filedatalake import DataLakeServiceClient, DataLakeFileClient

def decrypt_landing_to_raw(file_client:DataLakeFileClient, filename:str, secret_key:str,):
    # Data Configuration
    DATABASE_NAME = os.environ["DATABASE_NAME"]
    DATASET_NAME = os.environ["DATASET_NAME"]
    EXTRACTION_DATE = os.environ["EXTRACTION_DATE"]
    # Azure Configuration
    AZURE_STORAGE_ACCOUNT = os.environ["AZURE_STORAGE_ACCOUNT"]
    AZURE_STORAGE_ACCESS_KEY = os.environ["AZURE_STORAGE_ACCESS_KEY"]
    AZURE_FILE_SYSTEM = os.environ["AZURE_FILE_SYSTEM"]

    service_client = DataLakeServiceClient(
        account_url=f"https://{AZURE_STORAGE_ACCOUNT}.dfs.core.windows.net",
        credential=AZURE_STORAGE_ACCESS_KEY
    )
    file_system_client = service_client.get_file_system_client(file_system=AZURE_FILE_SYSTEM)

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

    # Decryption properties
    decryption_properties = crypto_factory.file_decryption_properties(kms_connection_config)

    # Download via path
    file_client = file_system_client.get_file_client(file_path=f'datalake/landing/{DATABASE_NAME}/{DATASET_NAME}/extraction_date={EXTRACTION_DATE}/{filename}')

    # Decrypt the downloaded Parquet file
    with file_client.download_file().readall() as f:
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
        output_folder = f'datalake/raw/{DATABASE_NAME}/{DATASET_NAME}/extraction_date={EXTRACTION_DATE}'
        file_system_client.create_directory(directory=output_folder)

        with pq.write_table(table, "test.parquet") as f:
            # file_client = file_system_client.get_file_client(file_path="test.parquet")
            file_client = file_system_client.create_file(f"{output_folder}/test.parquet")
            file_client.upload_data(f, overwrite=True)
            print(f"File test.parquet uploaded successfully.")
    except Exception as e:
        print(f'Error: {e}, File not uploaded.')