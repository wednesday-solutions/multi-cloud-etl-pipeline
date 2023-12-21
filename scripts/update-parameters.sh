#!/bin/bash

# Load environment variables from .custom-env file
source ./app/.custom-env

aws glue get-job --job-name main > scripts/job_details.json


# Update JSON file using jq
jq --arg GLUE_READ_PATH "$GLUE_READ_PATH" \
   --arg GLUE_WRITE_PATH "$GLUE_WRITE_PATH" \
   --arg KAGGLE_PATH "$KAGGLE_PATH" \
   '.Job.DefaultArguments["--GLUE_READ_PATH"] = $GLUE_READ_PATH |
    .Job.DefaultArguments["--GLUE_WRITE_PATH"] = $GLUE_WRITE_PATH |
    .Job.DefaultArguments["--KAGGLE_PATH"] = $KAGGLE_PATH' ./scripts/job_details.json > ./scripts/updated_job_details.json

