# Multi-cloud ETL Pipeline

## Main Objective

To run the same ETL code in multiple cloud services based on your preference, thus saving time & to develop the ETL scripts for different environments & clouds. Currently supports Azure Databricks + AWS Glue

## Note

- Azure Databricks can't be configured locally, you can only connect your local IDE to running cluster in databricks. Push your code in Github repo then make a workflow in databricks with URL of the repo & file.
- For AWS Glue I'm setting up a local environment using the Docker image, then deploying it to AWS glue using github actions.
- The "tasks.txt" file contents the details of transformations done in the main file.

## Requirements for Azure Databricks (for local connect only)
- [Unity Catalog](https://learn.microsoft.com/en-us/azure/databricks/data-governance/unity-catalog/enable-workspaces) enabled workspace.
- [Databricks Connect](https://learn.microsoft.com/en-us/azure/databricks/dev-tools/databricks-connect/python/install) configured on local machine. Running cluster.

## Requirements for AWS Glue (local setup)

- For Unix-based systems you can refer: [Data Engineering Onboarding Starter Setup](https://github.com/wednesday-solutions/Data-Engineering-Onboarding-Starter#setup)

- For Windows-based systems you can refer: [AWS Glue Developing using a Docker image](https://docs.aws.amazon.com/glue/latest/dg/aws-glue-programming-etl-libraries.html#develop-local-docker-image)

## Steps

1. Clone this repo in your own repo. For Windows recommend use WSL.

2. Give your S3, ADLS & Kaggle (optional) paths in the ```app/.custom_env``` file for Databricks. Make ```.evn``` file in the root folder for local Docker Glue to use.
Make sure to pass KAGGLE_KEY & KAGGLE_USERNAME values if you are going to use Kaggle. Else make the kaggle_extraction flag as False.

3. Run ```automation/init_docker_image.sh``` passing your aws credential location & project root location. If you are using Windows Powershell or CommandPrompt then run the commands manually by copy-pasting.

4. Write your jobs in the ```jobs``` folder. Refer ```demo.py``` file. One example is the ```jobs/main.py``` file.

5. Check your setup is correct, by running scripts in the docker container locally using ```spark-submit jobs/demo.py```. Make sure you see the "Execution Complete" statement printed.

## Deployment

1. In your your GitHub Actions Secrets, setup the following keys with their values:
    ```
    AWS_ACCESS_KEY_ID
    AWS_SECRET_ACCESS_KEY
    S3_BUCKET_NAME
    S3_SCRIPTS_PATH
    AWS_REGION
    AWS_GLUE_ROLE
    ```
    Rest all the key-value pairs that you wrote in your .env file, make sure you pass them using the ```automation/deploy_glue_jobs.sh``` file.

2. For Azure Databricks, make a workflow with the link of your repo & main file. Pass the following parameters with their correct values:

    ```
    kaggle_username
    kaggle_token
    storage_account_name
    datalake_access_key
    ```

## Documentation

[Multi-cloud Pipeline Documentation](https://docs.google.com/document/d/1npCpT_FIpw7ZuxAzQrEH3IsPKCDt7behmF-6VjrSFoQ/edit?usp=sharing)

## References

[Glue Programming libraries](https://docs.aws.amazon.com/glue/latest/dg/aws-glue-programming-python-libraries.html)

## Run Tests & Coverage Report

To run tests in the root of the directory use:

    coverage run --source=app -m unittest discover -s tests
    coverage report

Note that AWS Glue libraries are not available to download, so use AWS Glue 4 Docker container.
