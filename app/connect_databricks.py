import os


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
        print(f"{mount_path} Mount Successful")
    else:
        dbutils.fs.refreshMounts()
        print(f"{mount_path} Already mounted")
