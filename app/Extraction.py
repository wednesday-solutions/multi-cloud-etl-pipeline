import subprocess
import os
os.system('pip install kaggle')
import kaggle


def extract_from_kaggle(flag: bool):
    if flag:
        read_path = "/dbfs/mnt/rawdata/"
        write_path = "/mnt/transformed/"
    else:
        read_path = "temp/"
        write_path = "s3://glue-bucket-vighnesh/transformed/"

    # COMMAND ----------

    api = kaggle.KaggleApi()
    api.authenticate()
    api.dataset_download_cli("mastmustu/insurance-claims-fraud-data", unzip=True, path=read_path)

    if flag:
        zip_path = zip_path[5:]
        read_path = read_path[5:]
    else:
        read_path = "s3://glue-bucket-vighnesh/rawdata/"

    return read_path, write_path