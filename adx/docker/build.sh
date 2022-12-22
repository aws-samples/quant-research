#!/bin/bash
set -e 

if [[ $# -ne 2 ]]; then
    echo "usage: bash build.sh AWS_ACCOUNT_ID REGION"
    exit
fi

# AWS Account you deploy the solution
ACCOUNT=$1

#Region to deploy
REGION=$2


#full list https://docs.aws.amazon.com/emr/latest/EMR-on-EKS-DevelopmentGuide/docker-custom-images-tag.html
#declare -A EMR=( [ap-northeast-1]=059004520145 [ap-northeast-2]=996579266876 [ap-south-1]=235914868574 [ap-southeast-1]=671219180197 [ap-southeast-2]=038297999601 [ca-central-1]=351826393999 [eu-central-1]=107292555468 [us-east-1]=755674844232 [us-east-2]=711395599931 [us-west-1]=608033475327 [us-west-2]=895885662937 )


aws ecr get-login-password --region $REGION | docker login --username AWS --password-stdin ${EMR[$REGION]}.dkr.ecr.$REGION.amazonaws.com

docker pull ${EMR[$REGION]}.dkr.ecr.$REGION.amazonaws.com/notebook-spark/emr-6.7.0:latest

docker build -t me6.7_custom .

aws ecr create-repository --repository-name me6.7_custom_repo --image-scanning-configuration scanOnPush=true --region $REGION

aws ecr get-login-password --region $REGION | docker login --username AWS --password-stdin $ACCOUNT.dkr.ecr.$REGION.amazonaws.com

docker tag "me6.7_custom" $ACCOUNT.dkr.ecr.$REGION.amazonaws.com/me6.7_custom_repo

docker push $ACCOUNT.dkr.ecr.$REGION.amazonaws.com/me6.7_custom_repo