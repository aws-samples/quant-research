#!/bin/bash
# Submit BMLL data preparation pipeline to Anyscale workspace

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SRC_DIR="$(dirname "$SCRIPT_DIR")"

# Get workspace name
WORKSPACE_NAME=$(anyscale workspace_v2 list 2>/dev/null | grep -E "│.*│.*RUNNING" | awk -F'│' '{print $2}' | xargs)

if [ -z "$WORKSPACE_NAME" ]; then
    echo "No running workspace found. Please start your workspace first."
    exit 1
fi

JOB_NAME="bmll-dataprep-$(date +%Y%m%d-%H%M%S)"

echo "=========================================="
echo "BMLL Data Preparation Pipeline"
echo "=========================================="
echo "Job Name: $JOB_NAME"
echo "Workspace: $WORKSPACE_NAME"
echo "Working Directory: $SRC_DIR"
echo "=========================================="

cd "$SRC_DIR"
anyscale job submit \
  --name "$JOB_NAME" \
  --working-dir . \
  --requirements ../ray_infrastructure/requirements.txt \
  --wait \
  -- python pipeline_workflow/order_flow_bmll_dataprep.py
