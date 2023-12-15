import sys
from pyspark.context import SparkContext
from awsglue.utils import getResolvedOptions, GlueArgumentError
from awsglue.context import GlueContext


def init_glue():
    try:
        args = getResolvedOptions(
            sys.argv, ["JOB_NAME", "KAGGLE_USERNAME", "KAGGLE_KEY"]
        )
        print("\nRunning Glue Online\n")
    except GlueArgumentError:
        print("\nRunning Glue Locally\n")
        args = {"JOB_NAME": "local"}

    sc = SparkContext()
    glueContext = GlueContext(sc)
    spark = glueContext.spark_session

    return spark, args
