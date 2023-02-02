ARG ECR_ACCOUNT="755674844232"
ARG ECR_REGION="us-east-1"
ARG CONTAINER_TYPE="notebook-spark"
ARG CONTAINER_IMAGE="emr-6.8.0:latest"

FROM ${ECR_ACCOUNT}.dkr.ecr.${ECR_REGION}.amazonaws.com/${CONTAINER_TYPE}/${CONTAINER_IMAGE}
USER root
RUN pip3 install --upgrade pandas matplotlib sklearn numpy pyarrow s3fs bokeh vectorbt alpaca-py alpaca-py pyfolio
USER hadoop:hadoop