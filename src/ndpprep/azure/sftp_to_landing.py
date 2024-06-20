import os
import paramiko

from ndpprep.encryption.main import InMemoryKmsClient, pe

from azure.storage.filedatalake import DataLakeServiceClient, DataLakeFileClient

def sftp_to_landing(sftp_path:str):
    # Data Configuration
    DATABASE_NAME = os.environ["DATABASE_NAME"]
    DATASET_NAME = os.environ["DATASET_NAME"]
    EXTRACTION_DATE = os.environ["EXTRACTION_DATE"]
    # Azure Configuration
    AZURE_STORAGE_ACCOUNT = os.environ["AZURE_STORAGE_ACCOUNT"]
    AZURE_STORAGE_ACCESS_KEY = os.environ["AZURE_STORAGE_ACCESS_KEY"]
    AZURE_FILE_SYSTEM = os.environ["AZURE_FILE_SYSTEM"]
    # SFTP Configuration
    SFTP_HOST = os.environ["SFTP_HOST"]
    SFTP_USERNAME = os.environ["SFTP_USERNAME"]

    service_client = DataLakeServiceClient(
        account_url=f"https://{AZURE_STORAGE_ACCOUNT}.dfs.core.windows.net",
        credential=AZURE_STORAGE_ACCESS_KEY
    )
    file_system_client = service_client.get_file_system_client(file_system=AZURE_FILE_SYSTEM)

    ssh_client = paramiko.SSHClient()
    ssh_client.load_system_host_keys()
    ssh_client.connect(hostname=SFTP_HOST, username=SFTP_USERNAME)
    try:
        output_folder = f'datalake/landing/{DATABASE_NAME}/{DATASET_NAME}/extraction_date={EXTRACTION_DATE}'
        file_system_client = service_client.get_file_system_client(file_system=AZURE_FILE_SYSTEM)
        file_system_client.create_directory(directory=output_folder)

        sftp = ssh_client.open_sftp()
        with sftp.open(sftp_path, 'rb') as f:
            # file_client = file_system_client.get_file_client(file_path=f"{AZURE_STORAGE_PATH}/landing/{os.path.basename(sftp_path)}")
            file_client = file_system_client.create_file(f"{output_folder}/{os.path.basename(sftp_path)}")
            file_client.upload_data(f, overwrite=True)
            print(f"File {os.path.basename(sftp_path)} uploaded successfully.")

        ssh_client.close()
        # return the filename of the uploaded file
        return os.path.basename(sftp_path)
    except Exception as e:
        print(f'Error: {e}, File {os.path.basename(sftp_path)} not uploaded.')




