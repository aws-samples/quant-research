# Quant Research using AWS Batch

## Overview

This project deploys an AWS Batch infrastructure for Quant Research.

## Pre-requisites

The project has been built and tested using below configurations on `x86` architecture

a. Python - `v3.12`

 ```shell
 # Install pyenv
 curl https://pyenv.run | bash

 # Install Python 3.12
 pyenv install 3.12
 pyenv global 3.12
 ```

b. AWS CDK - `v2.1007.0`

 ```shell
 # Install NVM
 curl -o- https://raw.githubusercontent.com/nvm-sh/nvm/v0.40.1/install.sh | bash

 # Install Node LTS
 nvm install --lts

 # Install AWS CDK
 npm install -g aws-cdk@2.1018.0
 ```

c. Install `curl` and `jq`

 ```shell
 sudo dnf install curl jq
 ```

d. Install [Docker](https://docs.docker.com/get-started/get-docker/) for building containers

e. Install [AWS CLI](https://docs.aws.amazon.com/cli/latest/userguide/getting-started-install.html)

## Deployment

1. Export AWS credentials via CLI for your target environment
2. Create a GitHub Personal Access Token for allowing AWS CodePipeline to access the repository and build the container
   image. Refer
   the [GitHub documentation](https://docs.github.com/en/authentication/keeping-your-account-and-data-secure/managing-your-personal-access-tokens#creating-a-fine-grained-personal-access-token).
   You need to provide below permissions
    - Read access to code, commit statuses, and metadata
    - Read and Write access to repository hooks
3. **Optional:** If you enable `app_with_codepipeline` in [parameters.json](infrastructure/config/parameters.json). Create an AWS Secret with the GitHub personal access token value for triggering deployment via CI/CD for your
   application code
   ```shell
   aws secretsmanager create-secret \
   --name github-token \
   --description "GitHub PAT for the repository" \
   --secret-string "<GITHUB_PAT>"
   ```
4. Update the [.env](infrastructure/.env) file with the values for the infrastructure deployment. Below are the
   placeholder values provided in the [.env](infrastructure/.env) file for reference.
   ```shell
   # AWS account id and region where you need to deploy
   AWS_ACCOUNT_ID=012345678901
   AWS_REGION=us-east-1

   # Unique identifier used as a prefix for your AWS infrastructure resources
   NAMESPACE=quant-research-with-aws-batch
   
   # GITHUB prefixed variables are optional
   # Only needed if you enable `app_with_codepipeline` in parameters.json
   # The secret name is from Step#3
   GITHUB_OWNER=github-owner
   GITHUB_REPO=github-repo
   GITHUB_TOKEN_SECRET_NAME=github-token
   ```
5. If needed, modify the configurations provided in the [parameters.json](infrastructure/config/parameters.json)
6. Build the Python virtual environment
   ```shell
   python -m venv .venv
   source .venv/bin/activate
   cd infrastructure
   pip install -U -r requirements.txt
   cdk deploy --all
   ```

## Clean up

1. Delete all the stacks
   ```shell
   source .venv/bin/activate
   cd infrastructure
   cdk destroy --all
   ``` 
2. Delete the GitHub token secret stored in AWS Secrets Manager
3. You may need to manually delete resources like Amazon S3, if they contain data

# Security

See [CONTRIBUTING](./CONTRIBUTING.md#security-issue-notifications) for more information.

# License

This library is licensed under the MIT-0 License. See the [LICENSE](./LICENSE) file.