# Multi-cloud ETL Pipeline

## Main Objective

    To run the same ETL code in multiple cloud services based on your preference, thus saving time & to develop the ETL scripts for different environments & clouds.

## Note

- Azure Databricks can't be configured locally, you can only connect your local IDE to running cluster in databricks. Push your code in Github repo then make a workflow in databricks with URL of the repo & file.
- For AWS Glue I'm setting up a local environment using the Docker image, then deploying it to AWS glue using github actions.
- The "tasks.txt" file contents the details of transformations done in the main file.

## Requirements for Azure Databricks
- [Unity Catalog](https://learn.microsoft.com/en-us/azure/databricks/data-governance/unity-catalog/enable-workspaces) enabled workspace.
- [Databricks Connect](https://learn.microsoft.com/en-us/azure/databricks/dev-tools/databricks-connect/python/install) configured on local machine. Runing cluster.

## Steps for Azure Databricks

1. Clone this repo in your own repo.

2. For local IDE: Open configs file and write your own keys & storage accout name.
    For Databricks job: Configure job parameters, then call them using ```dbutils.widgets.get(param_name)```

3. Change your path for read & write in Extraction.py file, you can also change Extraction logic to use other sources.

4. (Optional) If you are connecting local IDE then uncomment the following line in the main file:
    ```
        spark, dbutils = cd.init_databricks()
    ```
    This fucntions connects to your cloud running cluster.
   
5. Just run on the local IDE to develop & test.

6. Commit & push the changes on Github.

7. Finally create workflow in databricks specifing your git repo & branch with your schedule. If your repo is private then generate github token & add it in databricks.

## Alternative for Azure Databricks

If you do not meet the requirments given above, no worries, you can develop your pipeline on databricks itself, just connect it using databricks repo for cicd & then create workflows.

## Requirements for AWS Glue (local setup)

- For Unix-based systems you can refer: [Data Enginnering Onboarding Starter](https://github.com/wednesday-solutions/Data-Engineering-Onboarding-Starter)

- For Windows-based systems you can refer: [AWS Glue Developing using a Docker image](https://docs.aws.amazon.com/glue/latest/dg/aws-glue-programming-etl-libraries.html#develop-local-docker-image)

## Step for AWS Glue

1. Clone this repo

2. For local developemnt: use a .env file and write your keys in it.
    For deploy on Glue: configure your keys in Github secrets so that they can be accessed in the actions.

3. Change your path for read & write in Extraction.py file, you can also change Extraction logic to use other sources.

4. Just run you code using ```spark-submit main.py```

5. Commit & push changes to Github.

6. Finally you need to give the keys as parameters in Glue Job. So that they can be accessed using ```args``` dictionary.

## IMP Note for AWS Glue

    Glue doens't recognize other files except the job file (i.e. main.py) so you have to give your pacakage as a separate wheel file or zip file & provide it's S3 uri in "Python library path". Refer [Glue Programming libraries](https://docs.aws.amazon.com/glue/latest/dg/aws-glue-programming-python-libraries.html)
    
