ARG ECR_ACCOUNT="755674844232"
ARG ECR_REGION="us-east-1"
ARG CONTAINER_TYPE="notebook-spark"
ARG CONTAINER_IMAGE="emr-6.8.0:latest"

FROM ${ECR_ACCOUNT}.dkr.ecr.${ECR_REGION}.amazonaws.com/${CONTAINER_TYPE}/${CONTAINER_IMAGE}
USER root
RUN yum install -y libpng-devel sqlite-devel xz-devel gcc-c++ openssl-devel bzip2-devel libffi-devel tar gzip wget make
RUN wget https://www.python.org/ftp/python/3.9.9/Python-3.9.9.tgz
RUN tar xzf Python-3.9.9.tgz 
RUN cd Python-3.9.9 && ./configure --enable-loadable-sqlite-extensions --enable-optimizations
RUN cd Python-3.9.9 && make altinstall
RUN pip3 freeze > requirements.txt
RUN pip3.9 install -r requirements.txt
RUN rm /usr/bin/python3
RUN ln -s /usr/local/bin/python3.9 /usr/bin/python3
ENV MPLLOCALFREETYPE 1
RUN pip3.9 install --upgrade matplotlib==3.2.2 kaleido backtrader pandas sklearn numpy pyarrow s3fs bokeh vectorbt pyEX alpaca-py pyfolio awswrangler boto3 yfinance
USER hadoop:hadoop