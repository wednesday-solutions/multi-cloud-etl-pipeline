# Databricks notebook source
from dotenv import load_dotenv

from pyspark.sql.functions import when, col
from pyspark.sql.functions import round as sp_round
from pyspark.sql import Window
import pyspark.sql.functions as F

import app.environment as env
import app.spark_wrapper as sw

load_dotenv("../app/.custom_env")  # Loading env for databricks
load_dotenv()  # Loading env for glue

# COMMAND ----------

if "dbutils" in locals():
    databricks = True
else:
    spark = None
    dbutils = None
    databricks = False

# COMMAND ----------
# fmt: off

# Keep this flag True if you want to extract data from kaggle, else False
kaggle_extraction = True

[employee, insurance, vendor] = env.get_data(databricks, kaggle_extraction, dbutils, spark) #pylint: disable=unbalanced-tuple-unpacking

write_path = env.get_write_path(databricks)

# fmt: on
# COMMAND ----------

# Write all your transformations from below to end-of-file

employee = sw.rename_same_columns(employee, "AGENT")
insurance = sw.rename_same_columns(insurance, "CUSTOMER")
vendor = sw.rename_same_columns(vendor, "VENDOR")

# COMMAND ----------

# task 1: creating one view

insurance_employee = insurance.join(employee, on="AGENT_ID", how="left")

df = insurance_employee.join(vendor, on="VENDOR_ID", how="left")

print("Task 1 Done")

# COMMAND ----------

# task 2: create new column 'colocation'

cond = (col("CUSTOMER_STATE") == col("INCIDENT_STATE")) & (
    col("AGENT_STATE") == col("INCIDENT_STATE")
)

df = df.withColumn("COLOCATION", when(cond, 1).otherwise(0))

print("Task 2 Done")

# task 3:
cond = (col("AUTHORITY_CONTACTED") != "Police") & (col("POLICE_REPORT_AVAILABLE") == 1)

df = df.withColumn(
    "AUTHORITY_CONTACTED", when(cond, "Police").otherwise(col("AUTHORITY_CONTACTED"))
)

print("Task 3 Done")

# COMMAND ----------

# task 4: create new column claim_deviation


sub = df.select(
    "TRANSACTION_ID", "INSURANCE_TYPE", "TXN_DATE_TIME", "CLAIM_AMOUNT"
).withColumn("TXN_DATE_TIME", F.unix_timestamp("TXN_DATE_TIME"))


window_spec = sw.make_window("INSURANCE_TYPE", "TXN_DATE_TIME", -30 * 86400, -1 * 86400)

sub = sub.withColumn(
    "AVG_30DAYS_CLAIM_AMOUNT", F.round(F.avg("CLAIM_AMOUNT").over(window_spec), 2)
)


window_prev = sw.make_window(
    "INSURANCE_TYPE", "TXN_DATE_TIME", Window.unboundedPreceding, 0
)

sub = sub.withColumn("MIN_TXN_DATE_TIME", F.min("TXN_DATE_TIME").over(window_prev))

sub = sub.withColumn(
    "DAYS_WITH_HISTORY",
    F.datediff(F.from_unixtime("TXN_DATE_TIME"), F.from_unixtime("MIN_TXN_DATE_TIME")),
)

sub = sub.withColumn(
    "DEVIATION",
    F.round(
        F.coalesce(F.col("AVG_30DAYS_CLAIM_AMOUNT"), F.lit(0)) / F.col("CLAIM_AMOUNT"),
        2,
    ),
)


cond1 = (F.col("DAYS_WITH_HISTORY") >= 30) & (F.col("DEVIATION") < 0.5)
cond2 = (F.col("DAYS_WITH_HISTORY") >= 30) & (F.col("DEVIATION") >= 0.5)

sub = sub.withColumn("CLAIM_DEVIATION", F.when(cond1, 1).when(cond2, 0).otherwise(-1))

claim_deviation = sub.select("TRANSACTION_ID", "CLAIM_DEVIATION")

df = df.join(claim_deviation, on="TRANSACTION_ID", how="left")

print("Task 4 Done")

# COMMAND ----------

# task 5: apply discounts & increments in claim_amount


def get_cond(type1, type2):
    return (col("INSURANCE_TYPE") == type1) | (col("INSURANCE_TYPE") == type2)


df = df.withColumn(
    "NEW_PREMIUM",
    when(get_cond("Mobile", "Travel"), sp_round(col("PREMIUM_AMOUNT") * 0.9, 2))
    .when(get_cond("Health", "Property"), sp_round(col("PREMIUM_AMOUNT") * 1.07, 2))
    .when(get_cond("Life", "Motor"), sp_round(col("PREMIUM_AMOUNT") * 1.02, 2))
    .otherwise("PREMIUM_AMOUNT"),
)

print("Task 5 Done")

# COMMAND ----------

# task 6: create new column 'eligible_for_discount'
cond = (
    (col("TENURE") > 60)
    & (col("EMPLOYMENT_STATUS") == "N")
    & (col("NO_OF_FAMILY_MEMBERS") >= 4)
)

df = df.withColumn("ELIGIBLE_FOR_DISCOUNT", when(cond, 1).otherwise(0))

print("Task 6 Done")


# task 7: create new column 'claim_velocity'
sub = df.select("TRANSACTION_ID", "INSURANCE_TYPE", "TXN_DATE_TIME").withColumn(
    "TXN_DATE_TIME", F.unix_timestamp("TXN_DATE_TIME")
)


window_30_days = sw.make_window(
    "INSURANCE_TYPE", "TXN_DATE_TIME", -30 * 86400, -1 * 86400
)

sub = sub.withColumn("30_days_count", F.count("TRANSACTION_ID").over(window_30_days))


window_3_days = sw.make_window(
    "INSURANCE_TYPE", "TXN_DATE_TIME", -3 * 86400, -1 * 86400
)

sub = sub.withColumn("3_days_count", F.count("TRANSACTION_ID").over(window_3_days))

sub = sub.withColumn(
    "CLAIM_VELOCITY", F.round(F.col("30_days_count") / F.col("3_days_count"), 2)
)

claim_velocity = sub.select("TRANSACTION_ID", "CLAIM_VELOCITY")

df = df.join(claim_velocity, on="TRANSACTION_ID", how="left")

print("Task 7 Done")

# COMMAND ----------

# task 8: find all suspicious employees
cond = (
    (col("CLAIM_STATUS") == "A")
    & (col("RISK_SEGMENTATION") == "H")
    & (col("INCIDENT_SEVERITY") == "Major Loss")
    & (col("CLAIM_AMOUNT") > 15000)
)

df = df.withColumn("SUSPICIOUS", when(cond, 1).otherwise(0))

print("Task 8 Done")

# COMMAND ----------

# finally writing the data in transformed container
df.coalesce(1).write.csv(write_path + "final_data.csv", header=True, mode="overwrite")

print("Execution Complete")
