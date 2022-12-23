@echo off

cd adx/docker
bash ./build.sh 614393260192 us-east-1
cd ../..
cdk bootstrap aws://614393260192/us-east-1
cd ~/IdeaProjects/awsquantresearch
cdk deploy
