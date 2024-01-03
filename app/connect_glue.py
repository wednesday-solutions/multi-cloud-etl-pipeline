import sys
from pyspark.context import SparkContext
from awsglue.utils import getResolvedOptions, GlueArgumentError
from awsglue.context import GlueContext


def init_glue():
    try:
        args = getResolvedOptions(
            sys.argv,
            [
                "JOB_NAME",
                "KAGGLE_USERNAME",
                "KAGGLE_KEY",
                "GLUE_READ_PATH",
                "GLUE_WRITE_PATH",
                "KAGGLE_PATH",
            ],
        )
        print("\nRunning Glue Online\n")
    except GlueArgumentError:
        print("\nRunning Glue Locally\n")
        args = {"JOB_NAME": "local"}

    sc = SparkContext()
    glue_context = GlueContext(sc)
    spark = glue_context.spark_session

    return spark, args
