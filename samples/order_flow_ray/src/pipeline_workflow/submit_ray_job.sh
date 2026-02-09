#!/bin/bash
# Submit Ray job to run BMLL data preparation pipeline

set -e

# Get the directory where this script is located
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
SRC_DIR="$(dirname "$SCRIPT_DIR")"

# Default values
SCRIPT_BASE=$(basename "$1" .py)

# Generate descriptive suffix based on parameters
if [ "$#" -eq 1 ]; then
  PARAMS_DESC="all"
else
  PARAMS_DESC=""
  for arg in "${@:2}"; do
    case "$arg" in
      --specific-date-types) PARAMS_DESC="${PARAMS_DESC}specific" ;;
      --files-slice) PARAMS_DESC="${PARAMS_DESC}slice" ;;
      [0-9]*) PARAMS_DESC="${PARAMS_DESC}${arg}" ;;
      *,*) PARAMS_DESC="${PARAMS_DESC}_files" ;;
    esac
  done
  [ -z "$PARAMS_DESC" ] && PARAMS_DESC="custom"
fi

JOB_NAME="${JOB_NAME:-${SCRIPT_BASE}_${PARAMS_DESC}_$(date +%Y%m%d-%H%M%S)}"
SPECIFIC_FILES="${2:-}"

# Check if Python script argument is provided
if [ -z "$1" ]; then
  echo "Error: Python script name is required"
  echo "Usage: $0 <python_script> [specific_files]"
  echo "Example: $0 order_flow_bmll_dataprep.py"
  echo "Example: $0 order_flow_repartition.py 's3://bucket/file1.parquet,s3://bucket/file2.parquet'"
  echo "Example: $0 order_flow_reconciliation.py --specific-date-types 2024-12-31,trades"
  echo "Example: $0 order_flow_reconciliation.py --specific-date-types 2024-12-31,trades 2024-12-31,level2q"
  echo "Example: $0 order_flow_reconciliation.py --files-slice 5"
  echo "Example: $0 order_flow_reconciliation.py  # Run all (default)"
  exit 1
fi

PYTHON_SCRIPT="pipeline_workflow/$1"

echo "Submitting Ray job: $JOB_NAME"
echo "Working directory: $SRC_DIR"
echo "Python script: $PYTHON_SCRIPT"
if [ -n "$SPECIFIC_FILES" ]; then
  echo "Specific files: $SPECIFIC_FILES"
else
  echo "Specific files: None (processing all discovered files)"
fi

# Submit the job
if [ "$#" -gt 1 ]; then
  # Pass all arguments after the script name
  ray job submit \
    --job-id="$JOB_NAME" \
    --working-dir="$SRC_DIR" \
    -- python "$PYTHON_SCRIPT" "${@:2}"
else
  ray job submit \
    --job-id="$JOB_NAME" \
    --working-dir="$SRC_DIR" \
    -- python "$PYTHON_SCRIPT"
fi

echo "Job submitted successfully: $JOB_NAME"
echo "Monitor job status with: ray job status $JOB_NAME"
echo "View job logs with: ray job logs $JOB_NAME"
