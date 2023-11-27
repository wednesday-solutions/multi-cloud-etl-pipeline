import subprocess


def extract_from_kaggle(flag: bool):
    command = "pip install kaggle"
    result = subprocess.run(command, shell=True, check=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
    print("Output:", result.stdout)

    # COMMAND ----------

    # downloading dataset zip file in zipdata container
    command = "kaggle datasets download -d mastmustu/insurance-claims-fraud-data"
    try:
        result = subprocess.run(command, shell=True, check=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
        print("Output:", result.stdout)
    except subprocess.CalledProcessError as e:
        print("Error:", e)
        print("Error Output:", e.stderr)

    # COMMAND ----------

    # unzip data in rawdata container
    command = f"unzip -o insurance-claims-fraud-data.zip -d temp/"
    result = subprocess.run(command, shell=True, check=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
    print("Output:", result.stdout)

    if flag:
        read_path = "/mnt/rawdata/"
        write_path = "/mnt/transformed/"
    else:
        read_path = "s3://glue-bucket-vighnesh/rawdata/"
        write_path = "s3://glue-bucket-vighnesh/transformed/"

    return read_path, write_path