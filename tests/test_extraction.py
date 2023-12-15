import unittest
from unittest.mock import patch
from app.extraction import extract_from_kaggle


class TestExtractFromKaggle(unittest.TestCase):
    @patch("app.extraction.os")
    @patch("app.extraction.kaggle")
    def test_successful_extraction_databricks(self, mock_kaggle, mock_os):
        # creating mock instances
        mock_kaggle_instance = mock_kaggle
        mock_kaggle_api = mock_kaggle_instance.KaggleApi.return_value
        mock_kaggle_api.authenticate.return_value = None

        mock_os.getenv.return_value = "path/to/kaggle/data"

        mock_kaggle_api.dataset_download_cli.return_value = None

        # calling the function
        extract_from_kaggle(databricks=True, extraction_path="/some/path")

        mock_kaggle.KaggleApi.assert_called_once()
        mock_kaggle_api.authenticate.assert_called_once()
        mock_kaggle_api.dataset_download_cli.assert_called_once_with(
            "path/to/kaggle/data", unzip=True, path="/dbfs/some/path"
        )
        mock_os.getenv.assert_called_once()
        mock_os.system.assert_not_called()  # since it's databricks os.system shouldn't be called

    @patch("app.extraction.os")
    @patch("app.extraction.kaggle")
    def test_successful_extraction_local(self, mock_kaggle, mock_os):
        # creating mock instances
        mock_kaggle_instance = mock_kaggle
        mock_kaggle_api = mock_kaggle_instance.KaggleApi.return_value
        mock_kaggle_api.authenticate.return_value = None

        mock_os.getenv.return_value = "path/to/kaggle/data"

        mock_kaggle_api.dataset_download_cli.return_value = None

        # calling the function
        extract_from_kaggle(databricks=False, extraction_path="path/for/extraction")

        mock_kaggle.KaggleApi.assert_called_once()
        mock_kaggle_api.authenticate.assert_called_once()
        mock_kaggle_api.dataset_download_cli.assert_called_once_with(
            "path/to/kaggle/data", unzip=True, path="temp/"
        )
        mock_os.getenv.assert_called_once()
        mock_os.system.assert_called_once_with(
            "aws s3 cp temp/ path/for/extraction --recursive"
        )

    @patch("app.extraction.os")
    @patch("app.extraction.kaggle")
    def test_failed_extraction_local(self, mock_kaggle, mock_os):
        # creating mock instances
        mock_kaggle_instance = mock_kaggle
        mock_kaggle_api = mock_kaggle_instance.KaggleApi.return_value
        mock_kaggle_api.authenticate.side_effect = SyntaxError(
            "Simulated Error in testing"
        )

        mock_kaggle_api.dataset_download_cli.return_value = None

        # calling the function
        with self.assertRaises(SyntaxError) as context:
            extract_from_kaggle(databricks=False, extraction_path="/invalid/path")

        mock_kaggle.KaggleApi.assert_called_once()
        mock_kaggle_api.authenticate.assert_called_once()

        # since authentication fialed followign will not be called
        mock_kaggle_api.dataset_download_cli.assert_not_called()
        mock_os.getenv.assert_not_called()
        mock_os.system.assert_not_called()

        actual_error_message = str(context.exception)
        self.assertTrue("Simulated Error" in actual_error_message)
