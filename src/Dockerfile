ARG ECR_ACCOUNT="755674844232"
ARG ECR_REGION="us-east-1"
ARG CONTAINER_TYPE="notebook-spark"
ARG CONTAINER_IMAGE="emr-6.10.0:latest"

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

RUN rm /usr/share/aws/iceberg/lib/iceberg-spark3-runtime.jar
RUN curl https://repo1.maven.org/maven2/software/amazon/awssdk/bundle/2.20.18/bundle-2.20.18.jar -Lo /usr/share/aws/aws-java-sdk/bundle-2.20.18.jar
RUN curl https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-spark-runtime-3.3_2.12/1.3.0/iceberg-spark-runtime-3.3_2.12-1.3.0.jar -Lo /usr/share/aws/iceberg/lib/iceberg-spark3-runtime.jar

RUN curl https://repo1.maven.org/maven2/software/amazon/awssdk/auth/2.20.18/auth-2.20.18.jar -Lo /usr/share/aws/aws-java-sdk/auth-2.20.18.jar
RUN curl https://repo1.maven.org/maven2/software/amazon/awssdk/auth-crt/2.20.18/auth-crt-2.20.18.jar -Lo /usr/share/aws/aws-java-sdk/auth-crt-2.20.18.jar
RUN curl https://repo1.maven.org/maven2/software/amazon/awssdk/crt/aws-crt/0.21.14/aws-crt-0.21.14.jar -Lo /usr/share/aws/aws-java-sdk/aws-crt-0.21.14.jar

USER hadoop:hadoop