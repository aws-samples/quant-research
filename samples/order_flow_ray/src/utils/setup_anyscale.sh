#!/bin/bash
# Get Anyscale workspace info and update .ray_config

set -e

echo "Fetching workspace info..."
echo ""

# Get workspace name
WORKSPACE_NAME=$(anyscale workspace_v2 list 2>/dev/null | grep -E "│.*│.*RUNNING" | awk -F'│' '{print $2}' | xargs)

echo "Workspace Name: $WORKSPACE_NAME"

if [ -z "$WORKSPACE_NAME" ]; then
    echo "No running workspace found"
    exit 1
fi

# Get cluster details
echo ""
echo "Getting cluster details..."
CLUSTER_INFO=$(anyscale workspace_v2 cluster list --name "$WORKSPACE_NAME" 2>/dev/null)
echo "$CLUSTER_INFO"

# Extract head node URL from cluster info
RAY_ADDRESS=$(echo "$CLUSTER_INFO" | grep -oE "[a-z0-9-]+\.anyscaleuserdata-staging\.com" | head -1)

if [ -z "$RAY_ADDRESS" ]; then
    # Try alternate command
    echo ""
    echo "Trying alternate method..."
    RAY_ADDRESS=$(anyscale workspace_v2 get --name "$WORKSPACE_NAME" 2>/dev/null | grep -oE "[a-z0-9-]+\.anyscaleuserdata[^[:space:]]*" | head -1)
fi

if [ -z "$RAY_ADDRESS" ]; then
    echo ""
    echo "Could not find Ray address. Manually set in .ray_config:"
    echo "RAY_ADDRESS=ray://<your-cluster-url>:10001"
    exit 1
fi

# Format as Ray address
RAY_ADDRESS="ray://${RAY_ADDRESS}:10001"

echo ""
echo "Ray Address: $RAY_ADDRESS"

# Update .ray_config
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SRC_DIR="$(dirname "$SCRIPT_DIR")"
CONFIG_FILE="$SRC_DIR/.ray_config"

echo "RAY_ADDRESS=$RAY_ADDRESS" > "$CONFIG_FILE"

echo ""
echo "✓ Updated $CONFIG_FILE"
echo ""
echo "Run: ./pipeline_workflow/order_flow_bmll_dataprep.sh"
