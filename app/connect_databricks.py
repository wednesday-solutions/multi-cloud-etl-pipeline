import app.configs as configs

from databricks.connect import DatabricksSession
from databricks.sdk import WorkspaceClient

storage_name = configs.storage_account_name
storage_key = configs.datalake_access_key


def init_databricks():
    spark = DatabricksSession.builder.getOrCreate()

    dbutils = WorkspaceClient().dbutils

    return spark, dbutils


def create_mount(dbutils, container_name, mount_path):
    mounts = [x.mountPoint for x in dbutils.fs.mounts()]
    try:
        if mount_path not in [x.mountPoint for x in dbutils.fs.mounts()]:
            dbutils.fs.mount(
            source = f'wasbs://{container_name}@{storage_name}.blob.core.windows.net/',
            mount_point = mount_path,
            extra_configs = {f'fs.azure.account.key.{storage_name}.blob.core.windows.net': storage_key})
            print(f"{mount_path} Mount Successfull")
        else:
            dbutils.fs.refreshMounts()
            print(f"{mount_path} Already mounted")
    except Exception as e:
        print(f"{mount_path} Error: " + e)


def unmount(dbutils, mount_path):
    try:
        dbutils.fs.unmount(mount_path)
        print("Unmount Successful")
    except Exception as e:
        print("Error: " + e)


