import unittest
from unittest.mock import patch
from app.Extraction import extract_from_kaggle

class TestExtraction(unittest.TestCase):

    @patch('app.Extraction.kaggle')
    def test_extract_from_kaggle_success(self, mock_kaggle):
        mock_kaggle_instance = mock_kaggle
        mock_api_instance = mock_kaggle_instance.KaggleApi.return_value
        # Mocking authenticate and dataset_download_cli methods
        mock_api_instance.authenticate.return_value = None
        mock_api_instance.dataset_download_cli.return_value = None

        # Call the function to test with flag=True for success case
        result = extract_from_kaggle(True)

        # Assertions
        mock_kaggle_instance.KaggleApi.assert_called_once()
        mock_api_instance.authenticate.assert_called_once()
        mock_api_instance.dataset_download_cli.assert_called_once_with(
            "mastmustu/insurance-claims-fraud-data", unzip=True, path="/dbfs/mnt/rawdata/"
        )

        self.assertEqual(result, ("/mnt/rawdata/", "/mnt/transformed/"))

    @patch('app.Extraction.kaggle')
    def test_extract_from_kaggle_failure(self, mock_kaggle):
        mock_kaggle_instance = mock_kaggle
        mock_api_instance = mock_kaggle_instance.KaggleApi.return_value
        # Mocking authenticate and dataset_download_cli methods
        mock_api_instance.authenticate.side_effect = Exception("Simulated authentication failure")

        # Call the function to test with flag=False for failure case
        with self.assertRaises(Exception) as context:
            extract_from_kaggle(False)

        # Assertions
        mock_kaggle_instance.KaggleApi.assert_called_once()
        mock_api_instance.authenticate.assert_called_once()
        mock_api_instance.dataset_download_cli.assert_not_called()

        # Check if the correct exception is raised
        self.assertEqual(str(context.exception), "Simulated authentication failure")
