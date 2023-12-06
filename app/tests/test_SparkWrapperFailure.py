from unittest import TestCase
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.errors import exceptions as E
from app.SparkWrapper import value_counts, rename_columns, create_frame, make_window


class TestSparkWrapper(TestCase):
    def setUp(self) -> None:
        self.spark = (
            SparkSession.builder.appName("Testing").master("local[*]").getOrCreate()
        )
        self.df = self.spark.read.csv(
            "app/tests/mock/sample.csv", inferSchema=True, header=True
        )
        super().setUp()

    def tearDown(self) -> None:
        self.spark.stop()
        super().tearDown()

    def test_value_counts_invalid_column(self):
        with self.assertRaises(E.captured.AnalysisException) as context:
            value_counts(self.df, "nonexistent_column")

        expected_error_message = "UNRESOLVED_COLUMN"
        actual_error_message = str(context.exception)
        self.assertTrue(expected_error_message in actual_error_message)

    def test_create_frame_invalid_path(self):
        with self.assertRaises(E.captured.AnalysisException) as context:
            create_frame(self.spark, "nonexistent_path/sample.csv")

        expected_error_message = "PATH_NOT_FOUND"
        actual_error_message = str(context.exception)

        self.assertTrue(expected_error_message in actual_error_message)

    def test_make_window_invalid_window_spec(self):
        with self.assertRaises(E.captured.AnalysisException) as context:
            window_spec = make_window("invalid_column", "date", -20, -1)
            self.df.withColumn("literal_1", F.lit(1).over(window_spec))

        expected_error_message = "UNRESOLVED_COLUMN"
        actual_error_message = str(context.exception)
        self.assertTrue(expected_error_message in actual_error_message)

    def test_make_window_invalid_range(self):
        with self.assertRaises(E.captured.AnalysisException) as context:
            window_spec = make_window("market", "date", 5, 2)
            self.df.withColumn("literal_1", F.lit(1).over(window_spec))

        expected_error_message = (
            "DATATYPE_MISMATCH.SPECIFIED_WINDOW_FRAME_WRONG_COMPARISON"
        )
        actual_error_message = str(context.exception)
        self.assertTrue(expected_error_message in actual_error_message)

    def test_rename_column_invalid_column(self):
        with self.assertRaises(ValueError) as context:
            rename_columns(self.df, {"invalid_col": "myname"})

        expected_error_message = "COLUMN DOESN'T EXIST"
        actual_error_message = str(context.exception)
        self.assertTrue(expected_error_message in actual_error_message)

    def test_rename_column_invalid_datatype(self):
        with self.assertRaises(TypeError) as context:
            rename_columns(self.df, ["invalid_col", "myname"])

        expected_error_message = "WRONG DATATYPE"
        actual_error_message = str(context.exception)
        self.assertTrue(expected_error_message in actual_error_message)
