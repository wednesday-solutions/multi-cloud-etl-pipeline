import subprocess
import os


def extract_from_kaggle(flag: bool):
    if flag:
        zip_path = "/mnt/zipdata/"
        read_path = "/mnt/rawdata/"
        write_path = "/mnt/transformed/"
    else:
        zip_path = "/"
        read_path = "temp/"
        write_path = "s3://glue-bucket-vighnesh/transformed/"

    os.system('pip install kaggle')

    # COMMAND ----------

    # downloading dataset zip file in zipdata container
    command = f"kaggle datasets download -d mastmustu/insurance-claims-fraud-data -p {zip_path}"
    result = subprocess.run(command, shell=True, check=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
    print("Output:", result.stdout)

    # COMMAND ----------

    # unzip data in rawdata container
    command = f"unzip -o {zip_path}/insurance-claims-fraud-data.zip -d {read_path}"
    result = subprocess.run(command, shell=True, check=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
    print("Output:", result.stdout)

    if flag==False:
        read_path = "s3://glue-bucket-vighnesh/rawdata/"

    return read_path, write_path