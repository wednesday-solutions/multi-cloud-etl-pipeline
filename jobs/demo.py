# This is demo file for writing your transformations
from dotenv import load_dotenv
import app.environment as env

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
# This is the example specific for "mastmustu/insurance-claims-fraud-data" data, different frames will be returned based on your data
# fmt: off

# Keep this flag True if you want to extract data from kaggle, else False
kaggle_extraction = False

[employee, insurance, vendor] = env.get_data(databricks, kaggle_extraction, dbutils, spark) #pylint: disable=unbalanced-tuple-unpacking

write_path = env.get_write_path(databricks)

# fmt: on
# COMMAND ----------

# Write all your transformations below:


print("\nExecution Complete\n")
