# Multi-cloud ETL Pipeline

## Table of Contents

* [Objective](#objective)
* [Note](#note)
* [Pre-requisite](#pre-requisite)
* [Set-up](#quick-start)
    * [Quick Start](#quick-start)
    * [Change your Paths](#change-your-paths)
    * [Setup Check](#setup-check)
* [Make New Jobs](#setup-check)
* [Deployment](#deployment)
* [Run Test & Coverage](#run-tests-&-coverage-report)
* [Documentation](Documentation.md#project-documentation)
* [Reference](#reference)
* [Common Errors](#common-errors)

## Objective

- To run the same ETL code in multiple cloud services based on your preference, thus saving time.
- To develop ETL scripts for different environments and clouds.

## Note

- This repository currently supports Azure Databricks + AWS Glue.
- Azure Databricks can't be configured locally, We can only connect our local IDE to running cluster in databricks. It works by pushing code in a Github repository then adding a workflow in databricks with URL of the repo & file.
- For AWS Glue we will set up a local environment using Glue Docker image or shell script, then deploying it to AWS glue using github actions.
- The "tasks.txt" file contains the details of transformations done in the main file.

## Pre-requisite

1. [Python3.7 with PIP](https://www.python.org/downloads/)
2. [AWS CLI configured locally](https://docs.aws.amazon.com/cli/latest/userguide/cli-configure-quickstart.html)
3. [Install Java 8](https://www.oracle.com/in/java/technologies/downloads/#java8-mac).
    ```bash
    # Make sure to export JAVA_HOME like this:
    export JAVA_HOME=/Library/Java/JavaVirtualMachines/jdk1.8.0_261.jdk/Contents/Home
    ```


## Quick Start

1. Clone this repo _(for Windows use WSL)_.

2. For setting up required libraries and packages locally, run:
```bash
    # If default SHELL is zsh use
    make setup-glue-local SOURCE_FILE_PATH=~/.zshrc

    # If default SHELL is bash use
    make setup-glue-local SOURCE_FILE_PATH=~/.bashrc
```

3. Source SHELL profile using:

```bash
    # For zsh
    source ~/.zshrc

    # For bash
    source ~/.bashrc
```

4. Install Dependencies:
```bash
    make install
```

## Change Your Paths

1. Enter your S3 & ADLS paths in the ```app/.custom_env``` file for Databricks. This file will be used by Databricks.

2. Similarly, we'll make ```.evn``` file in the root folder for Local Glue. To create the required file run:
```bash
    make glue-demo-env
```
This command will copy your paths from ```app/.custom_env``` to ```.env``` file.

3. _(Optional)_ If you want to extract from kaggle, enter KAGGLE_KEY & KAGGLE_USERNAME in ```.evn``` file only. Note: Don't enter any sensitive keys in ```app/.custom_env``` file.

## Setup Check
Finally, check if everything is working correctly by running:
```bash
    gluesparksubmit jobs/demo.py
```
Ensure "Execution Complete" is printed.

## Make New Jobs

Write your jobs in the ```jobs``` folder. Refer ```demo.py``` file. One example is the ```jobs/main.py``` file.

## Deployment

1. Set up a Github action for AWS Glue. Make sure to pass the following secrets in your repository:

```
    AWS_ACCESS_KEY_ID
    AWS_SECRET_ACCESS_KEY
    S3_BUCKET_NAME
    S3_SCRIPTS_PATH
    AWS_REGION
    AWS_GLUE_ROLE
```

Rest all the key-value pairs that entered in the `.env` file. make sure to pass them using `automation/deploy_glue_jobs.sh` file.

2. For Azure Databricks, make a workflow with the link to your repo & main file. Pass the following parameters with their correct values:

```
    kaggle_username
    kaggle_token
    storage_account_name
    datalake_access_key
```

## Run Tests & Coverage Report

To run tests & coverage report, run the following commands in the root folder of the project:

```bash
    make test

    # To see the coverage report
    make coverage-report
```

## References

[Glue Programming libraries](https://docs.aws.amazon.com/glue/latest/dg/aws-glue-programming-python-libraries.html)

## Common Errors

['sparkDriver' failed after 16 retries](https://stackoverflow.com/questions/52133731/how-to-solve-cant-assign-requested-address-service-sparkdriver-failed-after)
