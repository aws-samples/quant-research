#!/bin/bash
# Configure CodeCommit credentials from AWS Secrets Manager

set -e

SECRET_NAME="code_commit_awsbuilder"
REGION="us-east-1"
AWS_PROFILE="blitvinfdp"

echo "Retrieving CodeCommit credentials from Secrets Manager..."

# Get credentials from Secrets Manager
SECRET_JSON=$(aws secretsmanager get-secret-value \
    --secret-id "$SECRET_NAME" \
    --region "$REGION" \
    --profile "$AWS_PROFILE" \
    --query SecretString \
    --output text)

# Parse JSON to extract username and password
GIT_USERNAME=$(echo "$SECRET_JSON" | python3 -c "import sys, json; print(json.load(sys.stdin)['gitUsername'])")
GIT_PASSWORD=$(echo "$SECRET_JSON" | python3 -c "import sys, json; print(json.load(sys.stdin)['gitPassword'])")

echo "Configuring Git credentials for CodeCommit..."

# Configure git credential helper for CodeCommit
git config --global credential.helper '!aws codecommit credential-helper $@'
git config --global credential.UseHttpPath true

# Store credentials in git credential store
echo "https://$GIT_USERNAME:$GIT_PASSWORD@git-codecommit.us-east-1.amazonaws.com" | \
    git credential-store --file ~/.git-credentials store

echo "CodeCommit credentials configured successfully!"
echo "You can now clone/push to: https://git-codecommit.us-east-1.amazonaws.com/v1/repos/quant-research-sample-using-amazon-ecs-and-aws-batch"