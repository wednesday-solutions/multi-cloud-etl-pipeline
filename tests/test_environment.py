import subprocess
import unittest
from unittest.mock import patch, MagicMock
from app.environment import (
    set_keys_get_spark,
    get_dataframes,
    get_read_path,
    get_write_path,
    get_data,
)


class TestSetKeysGetSpark(unittest.TestCase):
    @patch("app.connect_glue.init_glue")
    @patch("app.environment.cd.create_mount")
    @patch("app.environment.dotenv.load_dotenv")
    def test_databricks_environment(
        self, mock_load_dotenv, mock_create_mount, mock_init_glue
    ):
        # Mocking dbutils and spark
        dbutils = MagicMock()
        spark = MagicMock()

        # Mocking the widgets
        dbutils.widgets.get = MagicMock(
            side_effect=lambda key: {
                "kaggle_username": "mock_username",
                "kaggle_token": "mock_token",
                "storage_account_name": "mock_account_name",
                "datalake_access_key": "mock_access_key",
            }[key]
        )

        # Call the function
        result_spark = set_keys_get_spark(True, dbutils, spark)

        # Assert
        self.assertEqual(result_spark, spark)
        dbutils.widgets.get.assert_called_with("datalake_access_key")
        mock_create_mount.assert_called_with(
            dbutils, "transformed", "/mnt/transformed/"
        )
        mock_init_glue.assert_not_called()
        mock_load_dotenv.assert_not_called()

    @patch("app.connect_glue.init_glue")
    @patch("app.environment.cd.create_mount")
    @patch("app.environment.dotenv.load_dotenv")
    def test_glue_local_environment(
        self, mock_load_dotenv, mock_create_mount, mock_init_glue
    ):
        # Mocking dbutils and spark
        dbutils = MagicMock()
        spark = MagicMock()

        mock_spark, mock_args = MagicMock(), {"JOB_NAME": "local"}
        mock_init_glue.return_value = (mock_spark, mock_args)

        # Call the function
        result_spark = set_keys_get_spark(False, dbutils, spark)

        # Assert
        self.assertEqual(result_spark, mock_spark)
        dbutils.widgets.get.assert_not_called()
        mock_create_mount.assert_not_called()
        mock_init_glue.assert_called_once()
        mock_load_dotenv.assert_called_once()

    @patch("app.connect_glue.init_glue")
    @patch("app.environment.cd.create_mount")
    @patch("app.environment.dotenv.load_dotenv")
    def test_glue_online_environment(
        self, mock_load_dotenv, mock_create_mount, mock_init_glue
    ):
        # Mocking dbutils and spark
        dbutils = MagicMock()
        spark = MagicMock()

        mock_spark, mock_args = MagicMock(), {
            "JOB_NAME": "online",
            "KAGGLE_USERNAME": "mock_name",
            "KAGGLE_KEY": "mock_key",
        }
        mock_init_glue.return_value = (mock_spark, mock_args)

        # mock_args['JOB_NAME'] = "local"

        # Call the function
        result_spark = set_keys_get_spark(False, dbutils, spark)

        # Assert
        self.assertEqual(result_spark, mock_spark)
        dbutils.widgets.get.assert_not_called()
        mock_create_mount.assert_not_called()
        mock_init_glue.assert_called_once()
        mock_load_dotenv.assert_not_called()

    @patch("app.environment.sw.create_frame")
    @patch("os.listdir")
    @patch("subprocess.run")
    def test_databricks_dataframes(self, mock_run, mock_listdir, mock_create_frame):
        # Mocking spark
        spark = MagicMock()

        # Mocking directory_path
        directory_path = "/mnt/rawdata"

        # Mocking csv_files
        mock_listdir.return_value = ["file1.csv", "file2.csv", "file3.parquet"]

        # Mock create_frame function
        mock_create_frame.return_value = MagicMock()

        # Call the function
        result_df_list = get_dataframes(True, spark, directory_path)

        # Assertions
        self.assertEqual(len(result_df_list), 2)
        mock_listdir.assert_called_with("/dbfs" + directory_path)
        mock_create_frame.assert_any_call(spark, "/mnt/rawdata/file1.csv")
        mock_create_frame.assert_any_call(spark, "/mnt/rawdata/file2.csv")
        mock_run.assert_not_called()

    @patch("app.environment.sw.create_frame")
    @patch("os.listdir")
    @patch("subprocess.run")
    def test_glue_dataframes(self, mock_run, mock_listdir, mock_create_frame):
        # Mocking spark
        spark = MagicMock()

        # Mocking directory_path
        directory_path = "/local/path"

        # Mocking subprocess result
        mock_run.return_value.stdout = "file1.csv\nfile2.csv\nfile3.json"

        # Mock create_frame function
        mock_create_frame.return_value = MagicMock()

        # Call the function
        result_df_list = get_dataframes(False, spark, directory_path)

        # Assertions
        self.assertEqual(len(result_df_list), 2)
        mock_run.assert_called_with(
            f"aws s3 ls {directory_path}",
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            shell=True,
            check=True,
        )
        mock_create_frame.assert_any_call(spark, "/local/path/file1.csv")
        mock_create_frame.assert_any_call(spark, "/local/path/file2.csv")
        mock_listdir.assert_not_called()

    @patch("app.environment.os.getenv")
    def test_read_path_databricks(self, mock_os_getenv):
        mock_os_getenv.return_value = "path/to/databricks_read"

        result = get_read_path(True)

        self.assertEqual(result, "path/to/databricks_read")
        mock_os_getenv.assert_called_once_with("DATABRICKS_READ_PATH")

    @patch("app.environment.os.getenv")
    def test_write_path_databricks(self, mock_os_getenv):
        mock_os_getenv.return_value = "path/to/databricks_write"

        result = get_write_path(True)

        self.assertEqual(result, "path/to/databricks_write")
        mock_os_getenv.assert_called_once_with("DATABRICKS_WRITE_PATH")

    @patch("app.environment.os.getenv")
    def test_read_path_glue(self, mock_os_getenv):
        mock_os_getenv.return_value = "path/to/glue_read"

        result = get_read_path(False)

        self.assertEqual(result, "path/to/glue_read")
        mock_os_getenv.assert_called_once_with("GLUE_READ_PATH")

    @patch("app.environment.os.getenv")
    def test_write_path_glue(self, mock_os_getenv):
        mock_os_getenv.return_value = "path/to/glue_write"

        result = get_write_path(False)

        self.assertEqual(result, "path/to/glue_write")
        mock_os_getenv.assert_called_once_with("GLUE_WRITE_PATH")

    @patch("app.environment.set_keys_get_spark")
    @patch("app.environment.get_read_path")
    @patch("app.environment.get_dataframes")
    @patch("app.extraction.extract_from_kaggle")
    def test_kaggle_extraction_enabled(
        self,
        mock_extract_from_kaggle,
        mock_get_dataframes,
        mock_get_read_path,
        mock_set_keys_get_spark,
    ):
        # Mocking parameters
        databricks = True
        kaggle_extraction = True
        dbutils = MagicMock()
        spark = MagicMock()

        # Mocking set_keys_get_spark function
        mock_set_keys_get_spark.return_value = spark

        # Mocking get_read_path function
        mock_get_read_path.return_value = "/mnt/rawdata"

        # Mocking extract_from_kaggle function
        mock_get_dataframes.return_value = [MagicMock(), MagicMock()]

        # Call the function
        result_data = get_data(databricks, kaggle_extraction, dbutils, spark)

        # Assertions
        self.assertEqual(len(result_data), 2)
        mock_set_keys_get_spark.assert_called_once_with(databricks, dbutils, spark)
        mock_get_read_path.assert_called_once_with(databricks)
        mock_extract_from_kaggle.assert_called_once_with(databricks, "/mnt/rawdata")
        mock_get_dataframes.assert_called_once_with(databricks, spark, "/mnt/rawdata")

    @patch("app.environment.set_keys_get_spark")
    @patch("app.environment.get_read_path")
    @patch("app.environment.get_dataframes")
    @patch("app.extraction.extract_from_kaggle")
    def test_kaggle_extraction_disabled(
        self,
        mock_extract_from_kaggle,
        mock_get_dataframes,
        mock_get_read_path,
        mock_set_keys_get_spark,
    ):
        # Mocking parameters
        databricks = False
        kaggle_extraction = False
        dbutils = MagicMock()
        spark = MagicMock()

        # Mocking set_keys_get_spark function
        mock_set_keys_get_spark.return_value = spark

        # Mocking get_read_path function
        mock_get_read_path.return_value = "/local/path"

        # Mocking extract_from_kaggle function
        mock_get_dataframes.return_value = [MagicMock(), MagicMock()]

        # Call the function
        result_data = get_data(databricks, kaggle_extraction, dbutils, spark)

        # Assertions
        self.assertEqual(len(result_data), 2)
        mock_set_keys_get_spark.assert_called_once_with(databricks, dbutils, spark)
        mock_get_read_path.assert_called_once_with(databricks)
        mock_extract_from_kaggle.assert_not_called()
        mock_get_dataframes.assert_called_once_with(databricks, spark, "/local/path")
