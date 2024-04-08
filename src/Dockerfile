ARG ECR_ACCOUNT="755674844232"
ARG ECR_REGION="us-east-1"
ARG CONTAINER_TYPE="notebook-spark"
ARG CONTAINER_IMAGE="emr-6.10.0:latest"

FROM ${ECR_ACCOUNT}.dkr.ecr.${ECR_REGION}.amazonaws.com/${CONTAINER_TYPE}/${CONTAINER_IMAGE}
USER root
RUN yum update -y
RUN yum install -y libpng-devel sqlite-devel xz-devel gcc-c++ openssl-devel bzip2-devel libffi-devel tar gzip wget make
RUN wget https://www.python.org/ftp/python/3.9.9/Python-3.9.9.tgz
RUN tar xzf Python-3.9.9.tgz
RUN mkdir /app && pip3 freeze > /app/requirements.txt
RUN sed -i 's/^pyzmq\(.*\)/pyzmq/' /app/requirements.txt
RUN cd Python-3.9.9 && ./configure --enable-loadable-sqlite-extensions --enable-optimizations
RUN cd Python-3.9.9 && make altinstall
RUN python3.9 -m pip install --upgrade pip
RUN rm /usr/bin/python3
RUN ln -s /usr/local/bin/python3.9 /usr/bin/python3
ENV MPLLOCALFREETYPE 1

RUN pip3.9 install -r /app/requirements.txt
RUN pip3.9 install --upgrade xgboost==2.0.0 lightgbm==4.1.0 catboost==1.2.2 pandas scikit-learn numpy matplotlib pyarrow kaleido bokeh
RUN pip3.9 install --upgrade backtrader vectorbt alpaca-py pyfolio pyEX yfinance
RUN pip3.9 install --upgrade dbl-tempo s3fs awswrangler boto3>=1.28.71 future

USER hadoop:hadoop
