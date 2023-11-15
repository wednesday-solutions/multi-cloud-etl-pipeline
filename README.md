# Multi-cloud ETL Pipeline

## Requirements
- [Unity Catalog](https://learn.microsoft.com/en-us/azure/databricks/data-governance/unity-catalog/enable-workspaces) enabled workspace.
- [Databricks Connect](https://learn.microsoft.com/en-us/azure/databricks/dev-tools/databricks-connect/python/install) configured on local machine. Runing cluster.

## Steps

1. Clone this repo in your own repo.

2. Open configs file and write your own keys & storage accout name.

3. (Optional) If you are running locally then uncomment the following line in the main file:
    ```
        spark, dbutils = cd.init_databricks()
    ```
    This fucntions connects to your cloud running cluster.
   
5. Just run on the local machine to develop & test. I have added a tasks.txt file to list the done trasformation in the main file.

6. Commit & push the changes on git hub.

7. Finally create workflow in databricks specifing your git repo & branch with your schedule. If your repo is private then generate github token & add it in databricks.

## Alternative

If you do not meet the requirments given above, no worries, you can develop your pipeline on databricks itself, just connect it using databricks repo for cicd & then create workflows.
    
