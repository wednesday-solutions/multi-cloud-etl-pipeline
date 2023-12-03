from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job


def init_glue():
    sc = SparkContext()
    glueContext = GlueContext(sc)
    spark = glueContext.spark_session
    job = Job(glueContext)

    return glueContext, spark, job
