#!/bin/bash

# Getting job-skeleton.json
aws glue update-job --generate-cli-skeleton > scripts/job-skeleton.json

# Extract values from updated_job_details.json
jobName=$(jq -r '.Job.Name' scripts/updated_job_details.json)
description=$(jq -r '.Job.Description' scripts/updated_job_details.json)
role=$(jq -r '.Job.Role' scripts/updated_job_details.json)
maxConcurrentRuns=$(jq -r '.Job.ExecutionProperty.MaxConcurrentRuns' scripts/updated_job_details.json)
commandName=$(jq -r '.Job.Command.Name' scripts/updated_job_details.json)
scriptLocation=$(jq -r '.Job.Command.ScriptLocation' scripts/updated_job_details.json)
pythonVersion=$(jq -r '.Job.Command.PythonVersion' scripts/updated_job_details.json)
workerType=$(jq -r '.Job.WorkerType' scripts/updated_job_details.json)
numberOfWorkers=$(jq -r '.Job.NumberOfWorkers' scripts/updated_job_details.json)
maxRetries=$(jq -r '.Job.MaxRetries' scripts/updated_job_details.json)
timeout=$(jq -r '.Job.Timeout' scripts/updated_job_details.json)
maxCapacity=$(jq -r '.Job.MaxCapacity' scripts/updated_job_details.json)
glueVersion=$(jq -r '.Job.GlueVersion' scripts/updated_job_details.json)
defaultArguments=$(jq -r '.Job.DefaultArguments' scripts/updated_job_details.json)

# Update update-job-skeleton.json with extracted values (excluding specified keys)
jq --arg jobName "$jobName" \
   --arg description "$description" \
   --arg role "$role" \
   --argjson maxConcurrentRuns "$maxConcurrentRuns" \
   --arg commandName "$commandName" \
   --arg scriptLocation "$scriptLocation" \
   --arg pythonVersion "$pythonVersion" \
   --arg workerType "$workerType" \
   --argjson numberOfWorkers "$numberOfWorkers" \
   --argjson maxRetries "$maxRetries" \
   --argjson timeout "$timeout" \
   --argjson maxCapacity "$maxCapacity" \
   --arg glueVersion "$glueVersion" \
   --argjson defaultArguments "$defaultArguments" \
   '.JobName = $jobName |
    .JobUpdate.Description = $description |
    .JobUpdate.Role = $role |
    .JobUpdate.ExecutionProperty.MaxConcurrentRuns = $maxConcurrentRuns |
    .JobUpdate.Command.Name = $commandName |
    .JobUpdate.Command.ScriptLocation = $scriptLocation |
    .JobUpdate.Command.PythonVersion = $pythonVersion |
    .JobUpdate.DefaultArguments = $defaultArguments |
    .JobUpdate.Timeout = $timeout |
    .JobUpdate.GlueVersion = $glueVersion |
    .JobUpdate.NumberOfWorkers = $numberOfWorkers |
    del(.JobUpdate.NonOverridableArguments) |
    del(.JobUpdate.Connections) |
    del(.JobUpdate.AllocatedCapacity) |
    del(.JobUpdate.MaxCapacity) |
    del(.JobUpdate.SecurityConfiguration) |
    del(.JobUpdate.NotificationProperty)' scripts/job-skeleton.json > scripts/updated-skeleton.json


# Updating the details using updated-skeleton
aws glue update-job --cli-input-json file://scripts/updated-skeleton.json