import os
import kaggle


def extract_from_kaggle(databricks: bool, extraction_path: str):
    if databricks:
        temp_path = "/dbfs" + extraction_path
    else:
        temp_path = "temp/"

    api = kaggle.KaggleApi()
    api.authenticate()
    api.dataset_download_cli(os.getenv("KAGGLE_PATH"), unzip=True, path=temp_path)

    if databricks is False:
        copy_command = f"aws s3 cp {temp_path} {extraction_path} --recursive"
        os.system(copy_command)

    print(f"Extracted Data Successfully in path: {extraction_path}")
