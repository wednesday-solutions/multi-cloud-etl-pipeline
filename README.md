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
For Databricks job: Add your parameters in a json file in a already mounted container, then read it in a dictionary.

3. Change your path for read & write in Extraction.py file, you can also change Extraction logic to use other sources.

4. (Optional) If you are NOT connecting local IDE then comment the following line in the main file:
    ```
        spark, dbutils = cd.init_databricks()
    ```
    This fucntions connects to your cloud running cluster.

    Also you need to make a Databricks CLI profile, refer: [Configure Databricks Workspace CLI](https://www.youtube.com/watch?v=h4L064NfMV0&ab_channel=WafaStudies)
    
    Also note that the ```keys.json``` file which i'm reading is in an already mounted container, so for other environments you have to load the keys directly or use an abstraction method like ```load_env()```
   
5. Just run on the local IDE to develop & test.

6. Commit & push the changes on Github.

7. Finally create workflow in databricks specifing your git repo & branch with your schedule. If your repo is private then generate github token & add it in databricks.

## Alternative for Azure Databricks

If you do not meet the requirments given above, no worries, you can develop your pipeline on databricks itself, just connect it using databricks repo for cicd & then create workflows.

## Requirements for AWS Glue (local setup)

- For Unix-based systems you can refer: [Data Enginnering Onboarding Starter Setup](https://github.com/wednesday-solutions/Data-Engineering-Onboarding-Starter#setup)

- For Windows-based systems you can refer: [AWS Glue Developing using a Docker image](https://docs.aws.amazon.com/glue/latest/dg/aws-glue-programming-etl-libraries.html#develop-local-docker-image)

## Step for AWS Glue

1. Clone this repo

2. For local developemnt: use a .env file and write your keys in it.
For deploy on Glue: configure your keys in Github secrets so that they can be accessed in the actions.

3. Change your path for read & write in Extraction.py file, you can also change Extraction logic to use other sources.

4. Just run you code using ```spark-submit main.py``` for docker container or ```gluesparksubmit main.py``` for setup environment.

5. Commit & push changes to Github.

6. Finally you need to give the keys as parameters in Glue Job. So that they can be accessed using ```args``` dictionary.

## IMP Note for AWS Glue

Glue doesn't recognize other files except the job file (i.e. main.py) so you have to give your pacakage as a separate wheel file or zip file & provide it's S3 uri in "Python library path".

Steps:

1. Run ```pip install setuptools wheel```

2. Then make a ```setup.py``` file, with the following demo code
    ```
    from setuptools import setup

    setup(
        packages=["app"],
        name="app",
        version="0.9",
        install_requires=[]
    )
    ```

3. Run ```python3 setup.py bdist_wheel``` for making the wheel file.

4. You will have a wheel file generated in "dist" folder.

5. Upload this wheel file in your S3 bucket then paste it's uri path in "Python Library path" in Glue Job details.

Refer: [Glue Programming libraries](https://docs.aws.amazon.com/glue/latest/dg/aws-glue-programming-python-libraries.html)

## Run Tests & Coverage Report

To run tests in the root of the directory use:

    coverage run -m unittest discover -v -s app/tests
    coverage report
