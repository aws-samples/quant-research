# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: MIT-0

# Use AWS Deep Learning Container with PyTorch pre-installed
FROM 763104351884.dkr.ecr.us-east-1.amazonaws.com/pytorch-training:2.3.0-gpu-py311-cu121-ubuntu20.04-sagemaker

# Set environment variables for better pip performance and resolve conflicts
ENV PIP_NO_CACHE_DIR=1
ENV PIP_DISABLE_PIP_VERSION_CHECK=1
ENV PYTHONUNBUFFERED=1
ENV PIP_ROOT_USER_ACTION=ignore

# First, upgrade pip and setuptools
RUN pip install --upgrade pip setuptools wheel

# Install all packages together to let pip resolve dependencies properly
# This avoids the issue where s3fs downgrades botocore
RUN pip install --no-cache-dir \
    # AWS packages - use latest compatible versions
    boto3>=1.38.0 \
    botocore>=1.38.0 \
    s3transfer>=0.13.0 \
    awscli>=1.40.0 \
    # Data processing packages
    polars>=0.20.0 \
    pyiceberg \
    # Use newer s3fs that's compatible with latest boto3
    s3fs>=2024.6.0 \
    joblib>=1.3.2 \
    pytz>=2023.3 \
    mlflow>=2.0.0 \
    seaborn==0.12.2

# Copy application code (do this last for better layer caching)
COPY samples/order_flow/src/ /app/src/
COPY samples/order_flow/configs/ /app/configs/

# Set working directory
WORKDIR /app

# Create non-root user for security and prepare ML storage directories
RUN useradd -m -u 1000 appuser && \
    chown -R appuser:appuser /app && \
    mkdir -p /opt/ml-storage/datasets /opt/ml-storage/models && \
    chown -R appuser:appuser /opt/ml-storage
USER appuser

# Default command
CMD ["python3", "/app/src/batch_polars_dataprovider.py", "--help"]
