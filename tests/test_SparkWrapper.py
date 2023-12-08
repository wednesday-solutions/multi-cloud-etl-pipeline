from unittest import TestCase
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from app.SparkWrapper import value_counts, rename_columns, create_frame, make_window


class TestSparkWrapper(TestCase):
    def setUp(self) -> None:
        self.spark = (
            SparkSession.builder.master("local").appName("Testing").getOrCreate()
        )
        self.df = self.spark.read.csv(
            "app/tests/mock/sample.csv", inferSchema=True, header=True
        )
        super().setUp()

    def tearDown(self) -> None:
        self.spark.stop()
        super().tearDown()

    def test_value_counts(self):
        df = value_counts(self.df, "market")
        data = df.collect()

        expected_data = [
            {"market": "NYSE", "count": 5},
            {"market": "NASDAQ", "count": 5},
            {"market": "LSE", "count": 5},
        ]

        for actual, expected in zip(data, expected_data):
            for col_name in expected.keys():
                self.assertEqual(actual[col_name], expected[col_name])

    def test_rename_columns(self):
        df = rename_columns(
            self.df, {"stock_name": "stock", "market": "Market", "date": "Date"}
        )
        actual_columns = df.columns

        expected_columns = ["stock", "Market", "close_price", "Date"]

        self.assertListEqual(actual_columns, expected_columns)

    def test_create_frame(self):
        path = "app/tests/mock/sample.csv"
        df = create_frame(self.spark, path).drop("date")
        actual_data = df.collect()

        expected_data = [
            {"stock_name": "ABC Corp", "market": "NYSE", "close_price": 100.25},
            {"stock_name": "DEF Ltd", "market": "LSE", "close_price": 50.75},
            {"stock_name": "XYZ Inc", "market": "NASDAQ", "close_price": 75.5},
            {"stock_name": "ABC Corp", "market": "NYSE", "close_price": 95.2},
            {"stock_name": "DEF Ltd", "market": "LSE", "close_price": 55.4},
            {"stock_name": "XYZ Inc", "market": "NASDAQ", "close_price": 80.1},
            {"stock_name": "ABC Corp", "market": "NYSE", "close_price": 105.8},
            {"stock_name": "DEF Ltd", "market": "LSE", "close_price": 60.2},
            {"stock_name": "XYZ Inc", "market": "NASDAQ", "close_price": 92.4},
            {"stock_name": "ABC Corp", "market": "NYSE", "close_price": 110.5},
            {"stock_name": "DEF Ltd", "market": "LSE", "close_price": 68.75},
            {"stock_name": "XYZ Inc", "market": "NASDAQ", "close_price": 102.6},
            {"stock_name": "ABC Corp", "market": "NYSE", "close_price": 115.75},
            {"stock_name": "DEF Ltd", "market": "LSE", "close_price": 75.3},
            {"stock_name": "XYZ Inc", "market": "NASDAQ", "close_price": 112.2},
        ]

        for actual, expected in zip(actual_data, expected_data):
            for col_name in expected.keys():
                self.assertEqual(actual[col_name], expected[col_name])

    def test_make_window(self):
        sub = self.df.withColumn("date", F.unix_timestamp("date", "yyyy-MM-dd") / 86400)

        window_spec = make_window("market", "date", -20, -1)

        df = (
            sub.withColumn(
                "last_20_days_close_avg", F.avg("close_price").over(window_spec)
            )
            .orderBy(["date", "stock_name"])
            .select("close_price", "last_20_days_close_avg")
        )

        actual_data = df.collect()

        expected_data = [
            {"close_price": 100.25, "last_20_days_close_avg": None},
            {"close_price": 50.75, "last_20_days_close_avg": None},
            {"close_price": 75.5, "last_20_days_close_avg": None},
            {"close_price": 95.2, "last_20_days_close_avg": 100.25},
            {"close_price": 55.4, "last_20_days_close_avg": 50.75},
            {"close_price": 80.1, "last_20_days_close_avg": 75.5},
            {"close_price": 105.8, "last_20_days_close_avg": 97.725},
            {"close_price": 60.2, "last_20_days_close_avg": 53.075},
            {"close_price": 92.4, "last_20_days_close_avg": 77.8},
            {"close_price": 110.5, "last_20_days_close_avg": 100.5},
            {"close_price": 68.75, "last_20_days_close_avg": 57.8},
            {"close_price": 102.6, "last_20_days_close_avg": 86.25},
            {"close_price": 115.75, "last_20_days_close_avg": 108.15},
            {"close_price": 75.3, "last_20_days_close_avg": 64.475},
            {"close_price": 112.2, "last_20_days_close_avg": 97.5},
        ]

        for actual, expected in zip(actual_data, expected_data):
            for col_name in expected.keys():
                self.assertEqual(actual[col_name], expected[col_name])
