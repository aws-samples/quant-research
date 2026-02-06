#!/bin/bash
# Submit Ray job to run BMLL data preparation pipeline

set -e

# Get the directory where this script is located
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
SRC_DIR="$(dirname "$SCRIPT_DIR")"

# Default values
RAY_ADDRESS="${RAY_ADDRESS}"
if [ -z "$RAY_ADDRESS" ]; then
  echo "Error: RAY_ADDRESS environment variable must be set"
  echo "Example: export RAY_ADDRESS='http://your-cluster:8265'"
  exit 1
fi
JOB_NAME="${JOB_NAME:-bmll-dataprep-$(date +%Y%m%d-%H%M%S)}"
SPECIFIC_FILES="${2:-}"

# Check if Python script argument is provided
if [ -z "$1" ]; then
  echo "Error: Python script name is required"
  echo "Usage: $0 <python_script> [specific_files]"
  echo "Example: $0 order_flow_bmll_dataprep.py"
  echo "Example: $0 order_flow_repartition.py 's3://bucket/file1.parquet,s3://bucket/file2.parquet'"
  exit 1
fi

PYTHON_SCRIPT="pipeline_workflow/$1"

echo "Submitting Ray job: $JOB_NAME"
echo "Ray address: $RAY_ADDRESS"
echo "Working directory: $SRC_DIR"
echo "Python script: $PYTHON_SCRIPT"
if [ -n "$SPECIFIC_FILES" ]; then
  echo "Specific files: $SPECIFIC_FILES"
else
  echo "Specific files: None (processing all discovered files)"
fi

# Submit the job
if [ -n "$SPECIFIC_FILES" ]; then
  ray job submit \
    --address="$RAY_ADDRESS" \
    --job-id="$JOB_NAME" \
    --working-dir="$SRC_DIR" \
    -- python "$PYTHON_SCRIPT" "$SPECIFIC_FILES"
else
  ray job submit \
    --address="$RAY_ADDRESS" \
    --job-id="$JOB_NAME" \
    --working-dir="$SRC_DIR" \
    -- python "$PYTHON_SCRIPT"
fi

echo "Job submitted successfully: $JOB_NAME"
echo "Monitor job status with: ray job status $JOB_NAME"
echo "View job logs with: ray job logs $JOB_NAME"
