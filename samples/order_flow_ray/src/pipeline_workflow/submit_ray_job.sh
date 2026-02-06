#!/bin/bash
# Submit Ray job to run BMLL data preparation pipeline

set -e

# Get the directory where this script is located
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
SRC_DIR="$(dirname "$SCRIPT_DIR")"

# Default values
RAY_ADDRESS="${RAY_ADDRESS:-auto}"
JOB_NAME="${JOB_NAME:-bmll-dataprep-$(date +%Y%m%d-%H%M%S)}"

# Check if Python script argument is provided
if [ -z "$1" ]; then
  echo "Error: Python script name is required"
  echo "Usage: $0 <python_script>"
  echo "Example: $0 order_flow_bmll_dataprep.py"
  exit 1
fi

PYTHON_SCRIPT="pipeline_workflow/$1"

echo "Submitting Ray job: $JOB_NAME"
echo "Ray address: $RAY_ADDRESS"
echo "Working directory: $SRC_DIR"
echo "Python script: $PYTHON_SCRIPT"

# Submit the job
ray job submit \
  --address="$RAY_ADDRESS" \
  --job-id="$JOB_NAME" \
  --working-dir="$SRC_DIR" \
  -- python "$PYTHON_SCRIPT"

echo "Job submitted successfully: $JOB_NAME"
echo "Monitor job status with: ray job status $JOB_NAME"
echo "View job logs with: ray job logs $JOB_NAME"
