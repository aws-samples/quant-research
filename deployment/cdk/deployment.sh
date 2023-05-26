#!/bin/sh

if [[ $# -ne 2 ]]; then
    echo "usage: bash deployment.sh AWS_ACCOUNT_ID REGION"
    exit
fi

#install jq
sudo yum install -y jq 

# AWS Account you deploy the solution
ACCOUNT=$1

#Region to deploy
REGION=$2

export CDK_DEPLOY_ACCOUNT=$ACCOUNT
export CDK_DEPLOY_REGION=$REGION

npm install 

cdk bootstrap aws://${CDK_DEPLOY_ACCOUNT}/${CDK_DEPLOY_REGION}


#full list https://docs.aws.amazon.com/emr/latest/EMR-on-EKS-DevelopmentGuide/docker-custom-images-tag.html

JQ_FILTER='."'$REGION'"'
ECR_ACCOUNT=$(cat cdk.json | jq -r '.context."emr-on-eks-ecr-accounts"' | jq -r ${JQ_FILTER})     

aws ecr get-login-password --region $REGION | docker login --username AWS --password-stdin ${ECR_ACCOUNT}.dkr.ecr.$REGION.amazonaws.com

cdk deploy --all --require-approval=never 
