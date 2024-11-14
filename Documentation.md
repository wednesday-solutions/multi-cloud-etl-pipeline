# Project Documentation

[Back to Readme](README.md#table-of-contents)

## Table of Contents

* [connect_databricks](#connect_databricks)
    * [Overview](#overview)
    * [Module Components](#module-components)
    * [Example Usage](#example-usage)
    * [Notes](#notes)
* [connect_glue](#connect_glue)
    * [Overview](#overview-1)
    * [Module Components](#module-components-1)
    * [Example Usage](#example-usage-1)
    * [Notes](#notes-1)
* [spark_wrapper](#spark_wrapper)
    * [Overview](#overview-2)
    * [Module Components](#module-components-2)
    * [Example Usage](#example-usage-2)
    * [Notes](#notes-2)
* [environment](#environment)
    * [Overview](#overview-3)
    * [Module Components](#module-components-3)
    * [Example Usage](#example-usage-3)
    * [Notes](#notes-3)

## connect_databricks

#### Overview
The `connect_databricks` module provides a function, `create_mount`, for mounting an Azure Data Lake Storage container onto a Databricks environment. The module assumes the availability of necessary environment variables for storage account details.

#### Module Components
1. 
    ```py
    create_mount(dbutils, container_name, mount_path)
    ```
    **Description**:

    Creates a mount point for an Azure Data Lake Storage container within the Databricks environment. It checks if the mount point already exists and creates it if not.

    **Usage**:
    ```python
    create_mount(dbutils, container_name, mount_path)
    ```

    **Parameters**:

    ```dbutils```: The Databricks Utilities client.

    ```container_name```: The name of the Azure Data Lake Storage container.

    ```mount_path```: The desired mount point within the Databricks environment.

    **Returns**:

    None


    **Prints**:
    ```python
    If the mount is successful: "{mount_path} Mount Successful"
    If the mount already exists: "{mount_path} Already mounted"
    ```


#### Example Usage:
```python
import os
from databricks.connect_databricks import create_mount

# Set up your environment variables
os.environ["storage_account_name"] = "your_storage_account_name"
os.environ["datalake_access_key"] = "your_datalake_access_key"

# Specify the container name and mount path
container_name = "your_container_name"
mount_path = "/mnt/your_mount_path"

# Create a mount point
create_mount(dbutils, container_name, mount_path)
```

#### Notes:
Ensure that the required environment variables (storage_account_name and datalake_access_key) are set correctly.
The function assumes the use of Azure Data Lake Storage for mounting. Adjustments may be needed for other storage solutions.

## connect_glue

#### Overview:
The `connect_glue` module provides a function, `init_glue`, for initializing a Glue context. This function is designed to be used in AWS Glue jobs, either online or locally, and it retrieves necessary options such as job name, Kaggle username, and Kaggle key.

#### Module Components:
1. 
    ```py
    init_glue()
    ```
    **Description**:

    Initializes a Glue context for AWS Glue jobs. Detects whether the job is running online or locally and sets up the necessary Spark context.

    **Usage**:

    ```python
    spark, args = init_glue()
    ```

    **Parameters**:

    None

    **Returns**:

    ```spark```: The Spark session for AWS Glue.

    ```args```: A dictionary containing resolved options, including JOB_NAME, KAGGLE_USERNAME, and KAGGLE_KEY.

#### Example Usage:
```python
from connect_glue import init_glue

# Initialize Glue context
spark, args = init_glue()

# Access options
job_name = args["JOB_NAME"]
kaggle_username = args["KAGGLE_USERNAME"]
kaggle_key = args["KAGGLE_KEY"]
```


#### Notes:
Ensure that necessary options (JOB_NAME, KAGGLE_USERNAME, and KAGGLE_KEY) are provided when running the AWS Glue job. If not then the script would assumed to run locally.
This module assumes the use of AWS Glue for ETL operations. Adjustments may be needed for other contexts.


## spark_wrapper

#### Overview
The SparkWrapper module provides a set of utility functions for working with PySpark DataFrames using the SparkSession. It includes functions for creating DataFrames from CSV files, renaming columns, computing value counts, and creating window specifications for advanced window operations.

#### Module Components

1. 
    ```py
    create_frame(sc: SparkSession, path: str) -> DataFrame
    ```
    **Description**:

    This function creates a PySpark DataFrame by reading a CSV file from the specified path. It infers the schema and considers the first row as the header.

    **Usage**:
    ```py
    df = create_frame(spark_session, "/path/to/csv/file.csv")
    ```

    **Parameters**:

    ```sc```: The SparkSession instance.

    ```path```: The path to the CSV file.

    **Returns**:

    ```df```: The PySpark DataFrame.

2. 
    ```py
    rename_columns(df: DataFrame, names: dict) -> DataFrame
    ```
    **Description**:

    This function renames columns of a PySpark DataFrame based on the provided dictionary of old and new column names.

    **Usage**:
    ```py
    new_df = rename_columns(existing_df, {"old_name": "new_name", "another_old_name": "another_new_name"})
    ```

    **Parameters**:

    ```df```: The PySpark DataFrame.

    ```names```: A dictionary where keys are old column names and values are new column names.

    **Returns**:

    ```df```: The PySpark DataFrame with renamed columns.

3. 
    ```py
    value_counts(df: DataFrame, column: str) -> DataFrame
    ```
    **Description**:

    This function computes the value counts for a specific column in a PySpark DataFrame and orders the results in descending order based on count.

    **Usage**:

        ```python
        counts_df = value_counts(existing_df, "target_column")
        ```

    **Parameters**:

    ```df```: The PySpark DataFrame.

    ```column```: The target column for which value counts are computed.

    **Returns**:

    ```df```: A PySpark DataFrame with two columns - the target column and its respective count, ordered by count in descending order.

4. 
    ```py
    make_window(partition: str, order: str, range_from: int, range_to: int) -> Window
    ```
    **Description**:

    This function creates a PySpark Window specification for advanced window operations. It allows users to define partitioning, ordering, and a range for window functions.

    **Usage**:

    ```py
    window_spec = make_window("partition_column", "order_column", -1, 1)
    ```

    **Parameters**:

    ```partition```: The column used for partitioning.

    ```order```: The column used for ordering within partitions.

    ```range_from```: The lower bound of the window range.

    ```range_to```: The upper bound of the window range.

    **Returns**:

    ```window_spec```: The PySpark Window specification.

5. 
    ```py
    rename_same_columns(df: DataFrame, prefix: str) -> DataFrame
    ```

    **Description**:

    Renames specific columns in a PySpark DataFrame based on a predefined prefix. This function is customized for the mastmustu/insurance-claims-fraud-data dataset.

    **Usage**:
    ```py
    cleaned_df = rename_same_columns(df, prefix)
    ```

    **Parameters**:

    ```df```: PySpark DataFrame.

    ```prefix```: Prefix to be added to the column names.

    **Returns**:

    ```cleaned_df```: PySpark DataFrame with renamed columns.

#### Example Usage:
```py
from pyspark.sql import SparkSession
from spark_wrapper import create_frame, rename_columns, value_counts, make_window, rename_same_columns

# Create SparkSession
spark = SparkSession.builder.appName("SparkWrapperExample").getOrCreate()

# Example usage: Create DataFrame from CSV file
csv_path = "/path/to/your/file.csv"
data_frame = create_frame(spark, csv_path)

# Example usage: Rename columns
renamed_df = rename_columns(data_frame, {"old_name": "new_name", "another_old_name": "another_new_name"})

# Example usage: Compute value counts
counts_df = value_counts(data_frame, "target_column")

# Example usage: Create a Window specification
window_spec = make_window("partition_column", "order_column", -1, 1)

# Example usage: Rename same columns for insurance claim data
cleaned_df = rename_same_coumsn(old_df, "CUSTOMER")
```

#### Notes:
Ensure that a valid SparkSession (spark) is available before using these functions.
The create_frame function assumes that the CSV file has a header row, and it infers the schema.
Review the PySpark documentation for additional details on advanced window operations and DataFrame manipulations.

## environment

#### Overview:
The `environment` module provides functions for setting up and managing environment variables, obtaining Spark sessions, and retrieving data from specified paths. This module is designed to support both Databricks and local environments.

#### Module Components:

1. 
    ```py
    set_keys_get_spark(databricks: bool, dbutils, spark) -> spark
    ```
    
    **Description**:

    Sets environment variables and initializes Spark sessions based on the provided parameters. For Databricks environments, it also creates mounts for rawdata and transformed data.

    **Usage**:

    spark = set_keys_get_spark(databricks, dbutils, spark)

    **Parameters**:

    `databricks`: Boolean indicating whether the environment is Databricks.

    `dbutils`: Databricks Utilities client.

    `spark`: Spark session.

    **Returns**:

    `spark`: Initialized Spark session.

2. 
    ```py
    get_dataframes(databricks: bool, spark, directory_path: str) -> List[DataFrame]
    ```

    **Description**:

    Retrieves a list of PySpark DataFrames from CSV files in the specified directory. The list is populated with DataFrames created using the spark_wrapper.create_frame function.

    **Usage**:
    ```py
    df_list = get_dataframes(databricks, spark, directory_path)
    ```

    **Parameters**:

    `databricks`: Boolean indicating whether the environment is Databricks.

    `spark`: Spark session.

    `directory_path`: Path to the directory containing CSV files.

    **Returns**:

    `df_list`: List of PySpark DataFrames.

3. 
    ```py
    get_read_path(databricks: bool) -> str
    ```

    **Description**:

    Retrieves the read path based on the environment. For Databricks, it reads from the DATABRICKS_READ_PATH environment variable; for Glue, it reads from the GLUE_READ_PATH environment variable.

    **Usage**:
    ```py
    read_path = get_read_path(databricks)
    ```

    **Parameters**:

    `databricks`: Boolean indicating whether the environment is Databricks.

    **Returns**:

    `read_path`: Read path for data retrieval.

4. 
    ```py
    get_write_path(databricks: bool) -> str
    ```

    **Description**:

    Retrieves the write path based on the environment. For Databricks, it reads from the DATABRICKS_WRITE_PATH environment variable; for Glue, it reads from the GLUE_WRITE_PATH environment variable.

    **Usage**:

    write_path = get_write_path(databricks)

    **Parameters**:

    `databricks`: Boolean indicating whether the environment is Databricks.

    **Returns**:

    `write_path`: Write path for data storage.

5. 
    ```py
    get_data(databricks: bool, kaggle_extraction: bool, dbutils, spark) -> List[DataFrame]
    ```

    **Description**:
    
    Retrieves data from specified paths, optionally extracting data from Kaggle if kaggle_extraction is set to True.

    **Usage**:
    ```py
    data = get_data(databricks, kaggle_extraction, dbutils, spark)
    ```

    **Parameters**:

    `databricks`: Boolean indicating whether the environment is Databricks.

    `kaggle_extraction`: Boolean indicating whether to perform Kaggle data extraction.

    `dbutils`: Databricks Utilities client.

    `spark`: Spark session.

    **Returns**:

    `data`: List of PySpark DataFrames.

#### Example Usage:
```py
from pyspark.sql import SparkSession
from environment import set_keys_get_spark, get_data

# Create SparkSession
spark = SparkSession.builder.appName("EnvironmentExample").getOrCreate()

# Example usage: Set keys and get Spark session for Databricks
set_keys_get_spark(databricks=True, dbutils=dbutils, spark=spark)

# Example usage: Get data from specified paths
data_frames = get_data(databricks=True, kaggle_extraction=True, dbutils=dbutils, spark=spark)
```

#### Notes:
Ensure that environment variables and paths are correctly configured before using these functions.
For Databricks environments, make sure to set the required widgets using dbutils.widgets.
Review the PySpark documentation for additional details on working with Spark sessions and DataFrames.
