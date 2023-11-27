# Databricks notebook source
import os
os.system('pip install python-dotenv')
from dotenv import load_dotenv

# COMMAND ----------
try:
    flag = dbutils.widgets.get('flag')
except:
    load_dotenv()
    flag = os.environ['FLAG']

if flag == 'True':
    flag = True
else:
    flag = False

if flag:
    os.environ['KAGGLE_USERNAME'] = dbutils.widgets.get('kaggle_username')

    os.environ['KAGGLE_KEY'] = dbutils.widgets.get('kaggle_token')

    os.environ['storage_account_name'] = dbutils.widgets.get('storage_account_name')

    os.environ['datalake_access_key'] = dbutils.widgets.get('datalake_access_key')
else:
	pass

# COMMAND ----------
import SparkWrapper as sw
from Extraction import extract_from_kaggle

if flag:
    import connect_databricks as cd
else:
    import connect_glue as cg

# COMMAND ----------

if flag:
	# creating mounts
	cd.create_mount(dbutils, "zipdata", "/mnt/zipdata/")
	cd.create_mount(dbutils, "rawdata", "/mnt/rawdata/")
	cd.create_mount(dbutils, "transformed", "/mnt/transformed/")
else:
	# initiating glue spark
	glueContext, spark, job = cg.init_glue()
	job.init("sample")

# COMMAND ----------

import subprocess

read_path, write_path = extract_from_kaggle(flag)

if flag==False:
    copy_command = f"aws s3 cp temp/ {read_path} --recursive"
    result = subprocess.run(copy_command, shell=True, check=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
    print("Output:", result.stdout)

# COMMAND ----------

# reading data in different frames

employee = sw.create_frame(spark, read_path + 'employee_data.csv')
            
employee = sw.rename_columns(employee, {'ADDRESS_LINE1': 'AGENT_ADDRESS_LINE1', 'ADDRESS_LINE2': 'AGENT_ADDRESS_LINE2',\
                 'CITY': 'AGENT_CITY', 'STATE': 'AGENT_STATE', 'POSTAL_CODE': 'AGENT_POSTAL_CODE'})

# COMMAND ----------

insurance = sw.create_frame(spark, read_path + 'insurance_data.csv')
            
insurance = sw.rename_columns(insurance, {'ADDRESS_LINE1': 'CUSTOMER_ADDRESS_LINE1', \
    'ADDRESS_LINE2': 'CUSTOMER_ADDRESS_LINE2', 'CITY': 'CUSTOMER_CITY', 'STATE': 'CUSTOMER_STATE', \
        'POSTAL_CODE': 'CUSTOMER_POSTAL_CODE'})

# COMMAND ----------

vendor = sw.create_frame(spark, read_path + 'vendor_data.csv')
            
vendor = sw.rename_columns(vendor, {'ADDRESS_LINE1': 'VENDOR_ADDRESS_LINE1', 'ADDRESS_LINE2': 'VENDOR_ADDRESS_LINE2', \
                'CITY': 'VENDOR_CITY', 'STATE': 'VENDOR_STATE', 'POSTAL_CODE': 'VENDOR_POSTAL_CODE'})

# COMMAND ----------

# task 1: creating one view

insurance_employee = insurance.join(employee, on='AGENT_ID', how='left')

df = insurance_employee.join(vendor, on='VENDOR_ID', how='left')

print("Task 1 Done")

# COMMAND ----------

# task 2: create new column 'colocation'
from pyspark.sql.functions import when, col, mean

cond = ((col('CUSTOMER_STATE') == col('INCIDENT_STATE')) & (col('AGENT_STATE') == col('INCIDENT_STATE')))

df = df.withColumn('COLOCATION', when(cond, 1).otherwise(0))

print("Task 2 Done")

# task 3: 
cond = ((col('AUTHORITY_CONTACTED') != 'Police') & (col('POLICE_REPORT_AVAILABLE') == 1))

df = df.withColumn('AUTHORITY_CONTACTED', when(cond, 'Police').otherwise(col('AUTHORITY_CONTACTED')))

print("Task 3 Done")

# COMMAND ----------

# task 4: create new column claim_deviation

from pyspark.sql import Window
import pyspark.sql.functions as F


sub = df.select('TRANSACTION_ID', 'INSURANCE_TYPE', 'TXN_DATE_TIME', 'CLAIM_AMOUNT')\
        .withColumn("TXN_DATE_TIME", F.unix_timestamp('TXN_DATE_TIME'))


window_spec = sw.make_window('INSURANCE_TYPE', 'TXN_DATE_TIME', -30 * 86400, -1 * 86400)

sub = sub.withColumn('AVG_30DAYS_CLAIM_AMOUNT', F.round(F.avg('CLAIM_AMOUNT').over(window_spec), 2))


window_prev = sw.make_window('INSURANCE_TYPE', 'TXN_DATE_TIME', Window.unboundedPreceding, 0)

sub = sub.withColumn('MIN_TXN_DATE_TIME', F.min('TXN_DATE_TIME').over(window_prev))

sub = sub.withColumn('DAYS_WITH_HISTORY', F.datediff(F.from_unixtime('TXN_DATE_TIME'), F.from_unixtime('MIN_TXN_DATE_TIME')))

sub = sub.withColumn('DEVIATION', F.round(F.coalesce(F.col('AVG_30DAYS_CLAIM_AMOUNT'), F.lit(0)) / F.col('CLAIM_AMOUNT'), 2))


cond1 = ((F.col('DAYS_WITH_HISTORY') >= 30) & (F.col('DEVIATION') < 0.5))
cond2 = ((F.col('DAYS_WITH_HISTORY') >= 30) & (F.col('DEVIATION') >= 0.5))

sub = sub.withColumn('CLAIM_DEVIATION', F.when(cond1, 1).when(cond2, 0).otherwise(-1))

claim_deviation = sub.select('TRANSACTION_ID', 'CLAIM_DEVIATION')

df = df.join(claim_deviation, on = 'TRANSACTION_ID', how = 'left')

print("Task 4 Done")

# COMMAND ----------

# task 5: apply discounts & increments in claim_amount
from pyspark.sql.functions import round as sp_round

def get_cond(type1, type2):
    return ((col('INSURANCE_TYPE') == type1) | (col('INSURANCE_TYPE') == type2))



df = df.withColumn('NEW_PREMIUM', \
        when(get_cond('Mobile', 'Travel'), sp_round(col('PREMIUM_AMOUNT') * 0.9, 2))\
        .when(get_cond('Health', 'Property'), sp_round(col('PREMIUM_AMOUNT') * 1.07, 2))\
        .when(get_cond('Life', 'Motor'), sp_round(col('PREMIUM_AMOUNT') * 1.02, 2)).otherwise('PREMIUM_AMOUNT'))

print("Task 5 Done")

# COMMAND ----------

# task 6: create new column 'eligible_for_discount' 
cond = (col('TENURE') > 60) & (col('EMPLOYMENT_STATUS') == 'N') & (col('NO_OF_FAMILY_MEMBERS') >= 4)

df = df.withColumn('ELIGIBLE_FOR_DISCOUNT', when(cond, 1).otherwise(0))

print("Task 6 Done")


# task 7: create new column 'claim_velocity'
sub = df.select('TRANSACTION_ID', 'INSURANCE_TYPE', 'TXN_DATE_TIME')\
        .withColumn('TXN_DATE_TIME', F.unix_timestamp('TXN_DATE_TIME'))


window_30_days = sw.make_window('INSURANCE_TYPE', 'TXN_DATE_TIME', -30 * 86400, -1 * 86400)

sub = sub.withColumn('30_days_count', F.count('TRANSACTION_ID').over(window_30_days))


window_3_days = sw.make_window('INSURANCE_TYPE', 'TXN_DATE_TIME', -3 * 86400, -1 * 86400)

sub = sub.withColumn('3_days_count', F.count('TRANSACTION_ID').over(window_3_days))

sub = sub.withColumn('CLAIM_VELOCITY', F.round(F.col('30_days_count') / F.col('3_days_count'), 2))

claim_velocity = sub.select('TRANSACTION_ID', 'CLAIM_VELOCITY')

df = df.join(claim_velocity, on = 'TRANSACTION_ID', how = 'left')

print("Task 7 Done")

# COMMAND ----------

# task 8: find all suspicious employees
cond = ((col('CLAIM_STATUS') == 'A') & (col('RISK_SEGMENTATION') == 'H') & (col('INCIDENT_SEVERITY') == 'Major Loss') & (col('CLAIM_AMOUNT') > 15000))

df = df.withColumn('SUSPICIOUS', when(cond, 1).otherwise(0))

print("Task 8 Done")

# COMMAND ----------

# finally writting the data in transformed container
df.coalesce(1).write.csv(write_path + 'final_data.csv', header=True, mode="overwrite")

# COMMAND ----------

print("Execution Complete")
