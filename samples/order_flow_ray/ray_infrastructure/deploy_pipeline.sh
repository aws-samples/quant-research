#!/bin/bash
# Deploy CodeBuild pipeline for Ray Docker builds

set -e

echo "Deploying Ray Docker Pipeline..."

# Install CDK dependencies
pip install -r cdk_requirements.txt

# Deploy the stack
cdk deploy RayDockerPipelineStack --app "python cdk_stack.py" --profile blitvinfdp

echo "Pipeline deployed successfully!"
echo "Docker image will rebuild automatically on CodeCommit pushes to main branch"