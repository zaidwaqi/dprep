import os

from azure.storage.filedatalake import DataLakeServiceClient
from azure.identity import ManagedIdentityCredential


def check_number_of_parquet(db_name:str, dataset_name:str, extraction_date:str):
    # Initializing Configuration
    AZURE_STORAGE_ACCOUNT = os.environ["AZURE_STORAGE_ACCOUNT"]
    AZURE_FILE_SYSTEM = os.environ["AZURE_FILE_SYSTEM"]

    CREDENTIAL = ManagedIdentityCredential()
    service_client = DataLakeServiceClient(
        account_url=f"https://{AZURE_STORAGE_ACCOUNT}.dfs.core.windows.net",
        credential=CREDENTIAL,
    )
    file_system_client = service_client.get_file_system_client(
        file_system=AZURE_FILE_SYSTEM
    )
    parquet_paths = file_system_client.get_paths(file_path=f"{db_name}/{dataset_name}/extraction_date={extraction_date}")

    return len(parquet_paths)