import os
import subprocess
import dotenv
import app.connect_databricks as cd
import app.connect_glue as cg
import app.spark_wrapper as sw


def set_keys_get_spark(databricks: bool, dbutils, spark):
    if databricks:
        os.environ["KAGGLE_USERNAME"] = dbutils.widgets.get("kaggle_username")

        os.environ["KAGGLE_KEY"] = dbutils.widgets.get("kaggle_token")

        os.environ["storage_account_name"] = dbutils.widgets.get("storage_account_name")

        os.environ["datalake_access_key"] = dbutils.widgets.get("datalake_access_key")

        cd.create_mount(dbutils, "rawdata", "/mnt/rawdata/")
        cd.create_mount(dbutils, "transformed", "/mnt/transformed/")

    else:
        spark, args = cg.init_glue()
        if args["JOB_NAME"] == "local":
            dotenv.load_dotenv()
        else:
            os.environ["KAGGLE_USERNAME"] = args["KAGGLE_USERNAME"]
            os.environ["KAGGLE_KEY"] = args["KAGGLE_KEY"]

    return spark


def get_dataframes(databricks: bool, spark, directory_path: str):
    dfs = []

    if databricks:
        csv_files = [
            file for file in os.listdir(directory_path) if file.endswith(".csv")
        ]
    else:
        cmd = f"aws s3 ls {directory_path}"
        result = subprocess.run(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            shell=True,
            check=True,
        )
        lines = result.stdout.split("\n")
        csv_files = [line.split()[-1] for line in lines if line.endswith(".csv")]

    for csv_file in csv_files:
        file_path = os.path.join(directory_path, csv_file)
        df = sw.create_frame(spark, file_path)
        dfs.append(df)

    return dfs


def get_paths(databricks: bool):
    if databricks:
        return os.getenv("DATABRICKS_READ_PATH"), os.getenv("DATABRICKS_WRITE_PATH")

    return os.getenv("GLUE_READ_PATH"), os.getenv("GLUE_WRITE_PATH")
