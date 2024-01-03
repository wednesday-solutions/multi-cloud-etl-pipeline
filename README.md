# Multi-cloud ETL Pipeline

## Main Objective

To run the same ETL code in multiple cloud services based on your preference, thus saving time & to develop the ETL scripts for different environments & clouds. Currently supports Azure Databricks + AWS Glue

## Note

- Azure Databricks can't be configured locally, you can only connect your local IDE to running cluster in databricks. Push your code in Github repo then make a workflow in databricks with URL of the repo & file.
- For AWS Glue I'm setting up a local environment using the Docker image, then deploying it to AWS glue using github actions.
- The "tasks.txt" file contents the details of transformations done in the main file3.

## Requirements for Azure Databricks (for local connect only)
- [Unity Catalog](https://learn.microsoft.com/en-us/azure/databricks/data-governance/unity-catalog/enable-workspaces) enabled workspace.
- [Databricks Connect](https://learn.microsoft.com/en-us/azure/databricks/dev-tools/databricks-connect/python/install) configured on local machine. Runing cluster.

## Requirements for AWS Glue (local setup)

- For Unix-based systems you can refer: [Data Enginnering Onboarding Starter Setup](https://github.com/wednesday-solutions/Data-Engineering-Onboarding-Starter#setup)

- For Windows-based systems you can refer: [AWS Glue Developing using a Docker image](https://docs.aws.amazon.com/glue/latest/dg/aws-glue-programming-etl-libraries.html#develop-local-docker-image)

## Steps

1. Clone this repo in your own repo.

2. Give your s3, adlas & kaggle (optional) paths in the ```app/.custom-env``` file.

3. Just run a Glue 4 docker conatiner & write your transformations in ```main.py``` file. Install dependancies using ```pip install -r requirements.txt```

4. Run your scirpts in the docker container locally using ```spark-sumbit main.py```

## Deployemnt

1. In your AWS Glue job pass these parameters with thier correct values: 
    ```
    JOB_NAME
    KAGGLE_USERNAME
    KAGGLE_KEY
    GLUE_READ_PATH
    GLUE_WRITE_PATH
    KAGGLE_PATH (keep blank if not extracting)
    ```
    Rest everything is taken care of in ```cd.yml``` file.

2. For Azure Databricks, make a workflow with the link of your repo & main file. Pass the following parameters with their correct values:

    ```
    kaggle_username
    kaggle_token
    storage_account_name
    datalake_access_key
    ```

## Documentation

[Multi-cloud Pipeline Documnentation](https://docs.google.com/document/d/1npCpT_FIpw7ZuxAzQrEH3IsPKCDt7behmF-6VjrSFoQ/edit?usp=sharing)

## References

[Glue Programming libraries](https://docs.aws.amazon.com/glue/latest/dg/aws-glue-programming-python-libraries.html)

## Run Tests & Coverage Report

To run tests in the root of the directory use:

    coverage run --source=app -m unittest discover -s tests
    coverage report

Note that awsglue libraries are not availabe to download, so use AWS Glue 4 Docker container.
