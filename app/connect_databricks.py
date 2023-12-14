import os
from databricks.connect import DatabricksSession
from databricks.sdk import WorkspaceClient


def init_databricks():
    os.system("cp /dbfs/mnt/config/databricks-connect.txt ~/.databrickscfg")

    spark = DatabricksSession.builder.getOrCreate()

    dbutils = WorkspaceClient().dbutils

    return spark, dbutils

def get_param_value(dbutils, param_key):
    return dbutils.widgets.get(param_key)


def create_mount(dbutils, container_name, mount_path):
    storage_name = os.environ["storage_account_name"]
    storage_key = os.environ["datalake_access_key"]

    mounts = [x.mountPoint for x in dbutils.fs.mounts()]
    if mount_path not in mounts:
        dbutils.fs.mount(
            source=f"wasbs://{container_name}@{storage_name}.blob.core.windows.net/",
            mount_point=mount_path,
            extra_configs={
                f"fs.azure.account.key.{storage_name}.blob.core.windows.net": storage_key
            },
        )
        print(f"{mount_path} Mount Successfull")
    else:
        dbutils.fs.refreshMounts()
        print(f"{mount_path} Already mounted")


def unmount(dbutils, mount_path):
    try:
        dbutils.fs.unmount(mount_path)
        print("Unmount Successful")
    except FileNotFoundError:
        print(f"Error: Path not found - {mount_path}")
    except Exception as e:  # pylint: disable=W0718
        print(f"Error: {e}")
