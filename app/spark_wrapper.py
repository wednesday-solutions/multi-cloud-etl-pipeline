from pyspark.sql import DataFrame
from pyspark.sql import Window, WindowSpec


def create_frame(sc, path: str):
    df = sc.read.csv(path, inferSchema=True, header=True)
    return df


def rename_columns(df: DataFrame, names: dict) -> DataFrame:
    if not isinstance(names, dict):
        raise TypeError("WRONG DATATYPE: column names should be dictionary")

    columns = df.columns
    renamed_df = df
    for old_col, new_col in names.items():
        if old_col not in columns:
            raise ValueError(
                f"COLUMN DOESN'T EXIST: Column '{old_col}' does not exist in the DataFrame"
            )
        renamed_df = renamed_df.withColumnRenamed(old_col, new_col)

    return renamed_df


def value_counts(df: DataFrame, column: str) -> DataFrame:
    return df.groupBy(column).count().orderBy(["count", column], ascending=False)


def make_window(
    partition: str, order: str, range_from: int, range_to: int
) -> WindowSpec:
    return (
        Window.partitionBy(partition).orderBy(order).rangeBetween(range_from, range_to)
    )


# this is my custom cleaning function specific for mastmustu/insurance-claims-fraud-data data
def rename_same_columns(df: DataFrame, prefix: str) -> DataFrame:
    cols_dict = {
        "ADDRESS_LINE1": f"{prefix}_ADDRESS_LINE1",
        "ADDRESS_LINE2": f"{prefix}_ADDRESS_LINE2",
        "CITY": f"{prefix}_CITY",
        "STATE": f"{prefix}_STATE",
        "POSTAL_CODE": f"{prefix}_POSTAL_CODE",
    }

    df = rename_columns(df, cols_dict)

    return df
