import unittest
from unittest.mock import patch, MagicMock
from app.connect_glue import init_glue


class TestInitGlue(unittest.TestCase):
    @patch("app.connect_glue.SparkContext")
    @patch("app.connect_glue.GlueContext")
    @patch("app.connect_glue.Job")
    def test_init_glue(self, mock_job, mock_glue_context, mock_spark_context):
        # Mock the SparkContext, GlueContext, and Job
        mock_spark_context_instance = MagicMock()
        mock_glue_context_instance = MagicMock()
        mock_job_instance = MagicMock()

        # Set up the behavior of the mock instances
        mock_spark_context.return_value = mock_spark_context_instance
        mock_glue_context.return_value = mock_glue_context_instance
        mock_job.return_value = mock_job_instance

        # Call the function to test
        glue_context, spark, job = init_glue()

        # Assertions
        mock_spark_context.assert_called_once()
        mock_glue_context.assert_called_once_with(mock_spark_context_instance)
        mock_job.assert_called_once_with(mock_glue_context_instance)

        # Check if the returned values are correct
        self.assertEqual(glue_context, mock_glue_context_instance)
        self.assertEqual(spark, mock_glue_context_instance.spark_session)
        self.assertEqual(job, mock_job_instance)

    @patch("app.connect_glue.SparkContext")
    @patch("app.connect_glue.GlueContext")
    @patch("app.connect_glue.Job")
    def test_init_glue_failure(self, mock_job, mock_glue_context, mock_spark_context):
        # Simulate a ValueError during SparkContext initialization
        error_statement = "Simulated SparkContext initialization failure"
        mock_spark_context.side_effect = ValueError(error_statement)

        # Call the function to test
        with self.assertRaises(ValueError) as context:
            init_glue()

        # Assertions
        mock_spark_context.assert_called_once()
        mock_glue_context.assert_not_called()  # GlueContext should not be called if SparkContext initialization fails
        mock_job.assert_not_called()  # Job should not be called if SparkContext initialization fails

        # Check if the error displayed correctly
        self.assertEqual(str(context.exception), error_statement)
