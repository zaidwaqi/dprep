import os, sys
sys.path.append("src")
from ndpprep.secret_key import read_secret_key

TARGET_USER = "ndp"
TARGET_IP = "172.20.100.213"
TARGET_PATH = "/data"
DEFAULT_SECRET_PATH = "/home/cdsw/PIPELINE/secret_key.txt"

if os.environ.get("NDPPREP_MODE") == "development":
    SECRET_KEY = "randomsecretkey"
else:
    SECRET_KEY = read_secret_key(DEFAULT_SECRET_PATH)

#mapping_table_folder = f"/home/cdsw/PIPELINE/OUTPUT/mapping_table/{db}"