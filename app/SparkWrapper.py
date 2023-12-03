from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql import Window, WindowSpec


def create_frame(sc: SparkSession, path: str):
    df = sc.read.csv(path, inferSchema=True, header=True)
    return df


def rename_columns(df: DataFrame, names: dict) -> DataFrame:
    renamed_df = df
    for old_col, new_col in names.items():
        renamed_df = renamed_df.withColumnRenamed(old_col, new_col)
    return renamed_df


def value_counts(df: DataFrame, column: str) -> DataFrame:
    return df.groupBy(column).count().orderBy("count", ascending=False)


def make_window(
    partition: str, order: str, range_from: int, range_to: int
) -> WindowSpec:
    return (
        Window.partitionBy(partition).orderBy(order).rangeBetween(range_from, range_to)
    )
