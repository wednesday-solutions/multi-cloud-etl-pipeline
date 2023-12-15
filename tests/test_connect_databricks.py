import unittest
from unittest.mock import MagicMock, patch
from app.connect_databricks import create_mount


def constructor(self, mount_point):
    self.mountPoint = mount_point


MockMount = type("MockMount", (object,), {"__init__": constructor})


class TestMountFunctions(unittest.TestCase):
    def setUp(self):
        # Mocking dbutils
        self.dbutils = MagicMock()

    @patch(
        "app.connect_databricks.os.environ",
        {
            "storage_account_name": "mock_storage_account",
            "datalake_access_key": "mock_access_key",
        },
    )
    def test_create_mount_success(self):
        container_name = "mock_container"
        mount_path = "/mnt/mock_mount_point"

        # Mocking fs.mounts() to return an empty list
        self.dbutils.fs.mounts.return_value = []

        # Call the function to test
        create_mount(self.dbutils, container_name, mount_path)

        # Assertions
        self.dbutils.fs.mount.assert_called_once_with(
            source=f"wasbs://{container_name}@mock_storage_account.blob.core.windows.net/",
            mount_point=mount_path,
            extra_configs={
                "fs.azure.account.key.mock_storage_account.blob.core.windows.net": "mock_access_key"
            },
        )
        self.dbutils.fs.refreshMounts.assert_not_called()

    @patch(
        "app.connect_databricks.os.environ",
        {
            "storage_account_name": "mock_storage_account",
            "datalake_access_key": "mock_access_key",
        },
    )
    def test_create_mount_already_mounted(self):
        container_name = "mock_container"
        mount_path = "/mnt/mock_mount_point"

        # Mocking fs.mounts() to return a list with the mount path
        mocked_mount = MockMount(mount_path)
        self.dbutils.fs.mounts.return_value = [mocked_mount]

        # Call the function to test
        create_mount(self.dbutils, container_name, mount_path)

        # Assertions
        self.dbutils.fs.mount.assert_not_called()
        self.dbutils.fs.refreshMounts.assert_called_once()
