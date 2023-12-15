import unittest
import sys
from unittest.mock import patch, MagicMock
from app.connect_glue import init_glue


class TestInitGlue(unittest.TestCase):
    @patch("app.connect_glue.SparkContext")
    @patch("app.connect_glue.GlueContext")
    @patch("app.connect_glue.getResolvedOptions")
    def test_init_glue(
        self, mock_get_resolved_options, mock_glue_context, mock_spark_context
    ):
        # Mock the SparkContext, GlueContext
        mock_spark_context_instance = MagicMock()
        mock_glue_context_instance = MagicMock()

        # Set up the behavior of the mock instances
        mock_spark_context.return_value = mock_spark_context_instance
        mock_glue_context.return_value = mock_glue_context_instance

        # Set up the behavior of the getResolvedOptions mock
        mock_get_resolved_options.return_value = {
            "JOB_NAME": "test",
            "KAGGLE_USERNAME": "test_username",
            "KAGGLE_KEY": "test_key",
        }

        # Call the function to test
        spark, args = init_glue()

        # Assertions
        mock_spark_context.assert_called_once()
        mock_glue_context.assert_called_once_with(mock_spark_context_instance)
        mock_get_resolved_options.assert_called_once_with(
            sys.argv, ["JOB_NAME", "KAGGLE_USERNAME", "KAGGLE_KEY"]
        )

        # Check if the returned values are correct
        self.assertEqual(spark, mock_glue_context_instance.spark_session)
        self.assertEqual(
            args,
            {
                "JOB_NAME": "test",
                "KAGGLE_USERNAME": "test_username",
                "KAGGLE_KEY": "test_key",
            },
        )

    @patch("app.connect_glue.SparkContext")
    @patch("app.connect_glue.GlueContext")
    def test_init_glue_local(self, mock_glue_context, mock_spark_context):
        # Mock the SparkContext, GlueContext
        mock_spark_context_instance = MagicMock()
        mock_glue_context_instance = MagicMock()

        # Set up the behavior of the mock instances
        mock_spark_context.return_value = mock_spark_context_instance
        mock_glue_context.return_value = mock_glue_context_instance

        # Call the function to test
        spark, args = init_glue()

        # Assertions
        mock_spark_context.assert_called_once()
        mock_glue_context.assert_called_once_with(mock_spark_context_instance)

        # Check if the returned values are correct
        self.assertEqual(spark, mock_glue_context_instance.spark_session)
        self.assertEqual(args, {"JOB_NAME": "local"})

    @patch("app.connect_glue.SparkContext")
    @patch("app.connect_glue.GlueContext")
    @patch("app.connect_glue.getResolvedOptions")
    def test_init_glue_spark_context_failure(
        self, mock_get_resolved_options, mock_glue_context, mock_spark_context
    ):
        # Simulate a ValueError during SparkContext initialization
        error_statement = "Simulated SparkContext initialization failure"
        mock_spark_context.side_effect = ValueError(error_statement)

        # Set up the behavior of the getResolvedOptions mock
        mock_get_resolved_options.return_value = {
            "JOB_NAME": "test",
            "KAGGLE_USERNAME": "test_username",
            "KAGGLE_KEY": "test_key",
        }

        # Call the function to test
        with self.assertRaises(ValueError) as context:
            init_glue()

        # Assertions
        mock_spark_context.assert_called_once()
        mock_glue_context.assert_not_called()

        # Check if the error displayed correctly
        self.assertEqual(str(context.exception), error_statement)
