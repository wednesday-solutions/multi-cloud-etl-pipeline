import subprocess
import unittest
from unittest.mock import patch, MagicMock, Mock
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
    def test_databricks_environment_failure(
        self, mock_load_dotenv, mock_create_mount, mock_init_glue
    ):
        # Mocking dbutils and spark
        dbutils = MagicMock()
        spark = MagicMock()

        # Mocking the widgets
        dbutils.widgets.get = Mock(side_effect=Exception("Error fetching widget"))

        # Call the function
        with self.assertRaises(Exception) as context:
            set_keys_get_spark(True, dbutils, spark)

        # Assert
        self.assertEqual(str(context.exception), "Error fetching widget")
        dbutils.widgets.get.assert_any_call("kaggle_username")
        mock_create_mount.assert_not_called()
        mock_init_glue.assert_not_called()
        mock_load_dotenv.assert_not_called()

    @patch("app.connect_glue.init_glue")
    @patch("app.environment.cd.create_mount")
    @patch("app.environment.dotenv.load_dotenv")
    def test_glue_local_environment_failure(
        self, mock_load_dotenv, mock_create_mount, mock_init_glue
    ):
        # Mocking dbutils and spark
        dbutils = MagicMock()
        spark = MagicMock()

        # Mocking the init_glue function to raise an exception
        mock_init_glue.side_effect = Exception("Error initializing Glue")

        # Call the function
        with self.assertRaises(Exception) as context:
            set_keys_get_spark(False, dbutils, spark)

        # Assert
        self.assertEqual(str(context.exception), "Error initializing Glue")
        dbutils.widgets.get.assert_not_called()
        mock_create_mount.assert_not_called()
        mock_init_glue.assert_called_once()
        mock_load_dotenv.assert_not_called()

    @patch("app.environment.os.listdir")
    @patch("app.environment.sw.create_frame")
    def test_databricks_dataframes_failure(self, mock_create_frame, mock_listdir):
        # Mocking spark
        spark = MagicMock()

        # Mocking directory_path
        directory_path = "/mnt/rawdata"

        # Mocking csv_files to raise an exception
        mock_listdir.side_effect = Exception("Error listing directory")

        # Call the function
        with self.assertRaises(Exception) as context:
            get_dataframes(True, spark, directory_path)

        # Assert
        self.assertEqual(str(context.exception), "Error listing directory")
        mock_listdir.assert_called_with("/dbfs" + directory_path)
        mock_create_frame.assert_not_called()

    @patch("app.environment.sw.create_frame")
    @patch("subprocess.run")
    def test_glue_dataframes_failure(self, mock_run, mock_create_frame):
        # Mocking spark
        spark = MagicMock()

        # Mocking directory_path
        directory_path = "/local/path"

        # Mocking subprocess result to raise an exception
        mock_run.side_effect = subprocess.CalledProcessError(
            returncode=1, cmd="aws s3 ls ..."
        )

        # Call the function
        with self.assertRaises(subprocess.CalledProcessError) as context:
            get_dataframes(False, spark, directory_path)

        # Assert
        self.assertEqual(
            str(context.exception),
            "Command 'aws s3 ls ...' returned non-zero exit status 1.",
        )
        mock_run.assert_called_with(
            f"aws s3 ls {directory_path}",
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            shell=True,
            check=True,
        )
        mock_create_frame.assert_not_called()

    @patch("app.environment.os.getenv")
    def test_read_path_databricks_failure(self, mock_os_getenv):
        # Mocking os.getenv to return None
        mock_os_getenv.side_effect = Exception("Wrong key given")

        # Call the function
        with self.assertRaises(Exception) as context:
            get_read_path(True)

        # Assert
        self.assertEqual(str(context.exception), "Wrong key given")
        mock_os_getenv.assert_called_once_with("DATABRICKS_READ_PATH")

    @patch("app.environment.os.getenv")
    def test_write_path_glue_failure(self, mock_os_getenv):
        # Mocking os.getenv to return an empty string
        mock_os_getenv.side_effect = Exception("Wrong key given")

        # Call the function
        with self.assertRaises(Exception) as context:
            get_write_path(False)

        # Assert
        self.assertEqual(str(context.exception), "Wrong key given")
        mock_os_getenv.assert_called_once_with("GLUE_WRITE_PATH")

    @patch("app.environment.set_keys_get_spark")
    @patch("app.environment.get_read_path")
    @patch("app.environment.get_dataframes")
    @patch("app.extraction.extract_from_kaggle")
    def test_kaggle_extraction_failure(
        self,
        mock_extract_from_kaggle,
        mock_get_dataframes,
        mock_get_read_path,
        mock_set_keys_get_spark,
    ):
        # Mocking parameters
        databricks = False
        kaggle_extraction = True
        dbutils = MagicMock()
        spark = MagicMock()

        # Mocking set_keys_get_spark function
        mock_set_keys_get_spark.return_value = spark

        # Mocking get_read_path function
        mock_get_read_path.return_value = "/local/path"

        # Mocking Exception in kaggle_extraction
        mock_extract_from_kaggle.side_effect = Exception("Extraction Failed")

        # Call the function
        with self.assertRaises(Exception) as context:
            get_data(databricks, kaggle_extraction, dbutils, spark)

        # Assertions
        self.assertEqual(str(context.exception), "Extraction Failed")
        mock_set_keys_get_spark.assert_called_once_with(databricks, dbutils, spark)
        mock_get_read_path.assert_called_once_with(databricks)
        mock_extract_from_kaggle.assert_called_once_with(databricks, "/local/path")
        mock_get_dataframes.assert_not_called()
