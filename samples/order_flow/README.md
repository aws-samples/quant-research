# AWS Batch ML Pipeline for Order Flow and Orderbook Imbalance Analysis

This document explains how to deploy and run the machine learning pipeline on AWS Batch using your existing CDK infrastructure.

## 🏗️ Pipeline Architecture

The pipeline consists of two main stages:

1. **Data Preparation** (`batch_polars_dataprovider.py`):
   - Loads data from Iceberg tables
   - Generates advanced features using Polars 
   - Creates balanced training/validation/test datasets
   - Uploads datasets to S3 with structured paths

2. **Model Training** (`batch_train.py`):
   - Downloads datasets from S3
   - Trains multiple ML models (CNN_LSTM, RNN, etc.)
   - Uploads trained models to S3 with structured paths

## 📁 S3 Structure

Data and models are organized in S3 using this structure:

```
s3://your-bucket/
└── ml-pipeline/
    ├── datasets/
    │   └── instrument=72089/
    │       └── start_date=2024-01-01/
    │           └── end_date=2024-01-31/
    │               ├── train_dataset.pt
    │               ├── validation_dataset.pt
    │               ├── test_dataset.pt
    │               ├── scaler.pkl
    │               └── preparation_config.json
    └── models/
        └── instrument=72089/
            └── start_date=2024-01-01/
                └── end_date=2024-01-31/
                    ├── CNN_LSTM_best.pt
                    ├── RNN_best.pt
                    └── training_summary.json
```

## 🚀 Deployment Steps

### 1. Deploy CDK Infrastructure

First, ensure your CDK infrastructure is deployed:

```bash
cd infrastructure

# Install dependencies
pip install -r requirements.txt

# Configure environment variables
cp .env.template .env
# Edit .env with your AWS account details

# Deploy infrastructure
cdk deploy --all
```

This creates:
- VPC and networking
- S3 buckets (standard + S3 Express One Zone if enabled)
- AWS Batch compute environments and queues
- ECR repository
- CodePipeline (if enabled)

### 2. Build and Push Docker Image

Your existing Dockerfile should be updated to include the new batch scripts:

```dockerfile
FROM pytorch/pytorch:latest

# Install dependencies
COPY requirements.txt /app/
RUN pip install -r /app/requirements.txt

# Copy source code
COPY src/ /app/src/
COPY configs/ /app/configs/

# Set working directory
WORKDIR /app

# Default command
CMD ["python3", "/app/src/batch_polars_dataprovider.py", "--help"]
```

Build and push:

```bash
cd samples/order_flow

# Get ECR repository URI from CDK output
ECR_REPO_URI=$(aws cloudformation describe-stacks \
  --stack-name deployment-pipeline-stack-${NAMESPACE} \
  --query 'Stacks[0].Outputs[?OutputKey==`ECRRepositoryURI`].OutputValue' \
  --output text)

# Login to ECR
aws ecr get-login-password --region ${AWS_REGION} | \
  docker login --username AWS --password-stdin ${ECR_REPO_URI}

# Build and push
docker build -t ${ECR_REPO_URI}:latest .
docker push ${ECR_REPO_URI}:latest
```

### 3. Get Infrastructure Details

**First, make sure your environment variables are set:**

```bash
# Source your environment configuration
source infrastructure/.env

# Verify variables are set
echo "NAMESPACE: $NAMESPACE"
echo "AWS_REGION: $AWS_REGION"
echo "AWS_ACCOUNT_ID: $AWS_ACCOUNT_ID"
```

**Then get the job queue and job definition names from your CDK deployment:**

```bash
# Get CPU job queue name
CPU_JOB_QUEUE=$(aws batch describe-job-queues \
  --query "jobQueues[?starts_with(jobQueueName, \`${NAMESPACE}\`) && contains(jobQueueName, \`cpu\`)].jobQueueName" \
  --output text | head -1)

# Get GPU job queue name  
GPU_JOB_QUEUE=$(aws batch describe-job-queues \
  --query "jobQueues[?starts_with(jobQueueName, \`${NAMESPACE}\`) && contains(jobQueueName, \`gpu\`)].jobQueueName" \
  --output text | head -1)

# Get job definition names
CPU_JOB_DEFINITION=$(aws batch describe-job-definitions \
  --status ACTIVE \
  --query "jobDefinitions[?starts_with(jobDefinitionName, \`${NAMESPACE}\`) && contains(jobDefinitionName, \`cpu\`)].jobDefinitionName" \
  --output text | head -1)

GPU_JOB_DEFINITION=$(aws batch describe-job-definitions \
  --status ACTIVE \
  --query "jobDefinitions[?starts_with(jobDefinitionName, \`${NAMESPACE}\`) && contains(jobDefinitionName, \`gpu\`)].jobDefinitionName" \
  --output text | head -1)

# Use CPU job definition as default (works for both CPU and GPU queues)
JOB_DEFINITION=${CPU_JOB_DEFINITION:-$GPU_JOB_DEFINITION}

# Get S3 bucket name
S3_BUCKET=$(aws s3api list-buckets \
  --query "Buckets[?contains(Name, \`${NAMESPACE}\`)].Name" \
  --output text | head -1)

echo "CPU Job Queue: $CPU_JOB_QUEUE"
echo "GPU Job Queue: $GPU_JOB_QUEUE" 
echo "CPU Job Definition: $CPU_JOB_DEFINITION"
echo "GPU Job Definition: $GPU_JOB_DEFINITION"
echo "S3 Bucket: $S3_BUCKET"
```

**If the above commands return empty results, your infrastructure may not be deployed yet. Deploy it first:**

```bash
cd infrastructure
cdk deploy --all
```

**Alternative: List all Batch resources to find the correct names:**

```bash
# List all job queues
echo "Available Job Queues:"
aws batch describe-job-queues --query 'jobQueues[].jobQueueName' --output table

# List all job definitions
echo "Available Job Definitions:"
aws batch describe-job-definitions --status ACTIVE --query 'jobDefinitions[].jobDefinitionName' --output table

# List buckets with your namespace
echo "S3 Buckets containing '${NAMESPACE:-your-namespace}':"
aws s3api list-buckets --query "Buckets[?contains(Name, '${NAMESPACE:-your-namespace}')].Name" --output table
```

## 🏃‍♂️ Running the Pipeline

### Option 1: Using the Submission Script (Recommended)

```bash
cd samples/order_flow

# Configure your batch config
cp configs/batch_config.json.template configs/batch_config.json
# Edit configs/batch_config.json with your data sources

# Run complete pipeline for instrument 72089, April 2023
python scripts/submit_batch_pipeline.py \
  --config configs/batch_config.json \
  --data-prep-job-queue $CPU_JOB_QUEUE \
  --training-job-queue $GPU_JOB_QUEUE \
  --job-definition $JOB_DEFINITION \
  --s3-bucket $S3_BUCKET \
  --instrument 72089 \
  --start-date 2023-04-10 \
  --end-date 2023-04-11 \
  --models CNN_LSTM \
  --data-prep-memory 131072 \
  --data-prep-vcpus 16 \
  --training-memory 22528 \
  --training-vcpus 8 \
  --no-wait
```

### Option 2: Manual Job Submission

#### Data Preparation Job

```bash
aws batch submit-job \
  --job-name ml-pipeline-data-prep-72089-2024-01-01 \
  --job-queue your-job-queue-name \
  --job-definition your-job-definition-name \
  --container-overrides '{
    "resourceRequirements": [
      {
        "type": "MEMORY",
        "value": "65536"
      },
      {
        "type": "VCPU", 
        "value": "8"
      }
    ],
    "command": [
      "python3", "/app/src/batch_polars_dataprovider.py",
      "--config", "s3://your-bucket/configs/batch_config.json",
      "--output-dir", "/tmp/prepared_datasets"
    ]
  }'
```

#### Model Training Job

```bash
# Wait for data preparation to complete, then:
aws batch submit-job \
  --job-name ml-pipeline-training-72089-2024-01-01 \
  --job-queue your-job-queue-name \
  --job-definition your-job-definition-name \
  --container-overrides '{
    "resourceRequirements": [
      {
        "type": "MEMORY",
        "value": "131072"
      },
      {
        "type": "VCPU", 
        "value": "16"
      }
    ],
    "command": [
      "python3", "/app/src/batch_train.py",
      "--config", "s3://your-bucket/configs/batch_config.json",
      "--datasets-dir", "/tmp/prepared_datasets"
    ]
  }'
```

## 📊 Monitoring Jobs

### Check Job Status

```bash
# List running jobs for CPU queue
aws batch list-jobs --job-queue $CPU_JOB_QUEUE --job-status RUNNING

# List running jobs for GPU queue  
aws batch list-jobs --job-queue $GPU_JOB_QUEUE --job-status RUNNING

# List pending jobs (waiting for dependencies)
aws batch list-jobs --job-queue $GPU_JOB_QUEUE --job-status PENDING

# Describe specific job
JOB_ID="your-job-id-here"
aws batch describe-jobs --jobs $JOB_ID
```

### View Job Logs (Dynamic Method)

```bash
# Get job details and extract log information
JOB_ID="your-job-id-here"  # Replace with actual job ID

# Method 1: Get log group and stream from job description
JOB_DETAILS=$(aws batch describe-jobs --jobs $JOB_ID --output json)

# Extract log group name from logConfiguration
LOG_GROUP=$(echo $JOB_DETAILS | jq -r '.jobs[0].container.logConfiguration.options."awslogs-group"')

# Extract log stream name from container
LOG_STREAM=$(echo $JOB_DETAILS | jq -r '.jobs[0].container.logStreamName')

echo "Log Group: $LOG_GROUP"
echo "Log Stream: $LOG_STREAM"

# View logs
aws logs get-log-events \
  --log-group-name "$LOG_GROUP" \
  --log-stream-name "$LOG_STREAM" \
  --start-from-head

# Method 2: Alternative extraction with fallback
JOB_QUEUE=$(echo $JOB_DETAILS | jq -r '.jobs[0].jobQueue')

# Try to get actual log group from job configuration first
ACTUAL_LOG_GROUP=$(echo $JOB_DETAILS | jq -r '.jobs[0].container.logConfiguration.options."awslogs-group" // empty')

if [ -n "$ACTUAL_LOG_GROUP" ] && [ "$ACTUAL_LOG_GROUP" != "null" ]; then
    LOG_GROUP_NAME="$ACTUAL_LOG_GROUP"
else
    # Fall back to determining from job queue if not available
    if [[ $JOB_QUEUE == *"cpu"* ]]; then
        LOG_GROUP_NAME="/${NAMESPACE}/batch-job/single-node-with-cpu"
    elif [[ $JOB_QUEUE == *"gpu"* ]]; then
        LOG_GROUP_NAME="/${NAMESPACE}/batch-job/multi-node-with-gpu"
    fi
fi

# Get the container log stream
CONTAINER_LOG_STREAM=$(echo $JOB_DETAILS | jq -r '.jobs[0].container.logStreamName')

echo "Log Group: $LOG_GROUP_NAME"
echo "Log Stream: $CONTAINER_LOG_STREAM"

# View logs with extracted names
aws logs get-log-events \
  --log-group-name "$LOG_GROUP_NAME" \
  --log-stream-name "$CONTAINER_LOG_STREAM" \
  --start-from-head
```

### Complete Monitoring Script

```bash
#!/bin/bash
# monitor_job.sh - Complete job monitoring script

JOB_ID="$1"
if [ -z "$JOB_ID" ]; then
    echo "Usage: $0 <job-id>"
    exit 1
fi

# Source environment
source infrastructure/.env

echo "=== Monitoring Job: $JOB_ID ==="

# Get job details
echo "📋 Job Status:"
aws batch describe-jobs --jobs $JOB_ID \
  --query 'jobs[0].{Status:jobStatus,Queue:jobQueue,Definition:jobDefinition,CreatedAt:createdAt}' \
  --output table

# Get log information
echo -e "\n📝 Getting log details..."
JOB_DETAILS=$(aws batch describe-jobs --jobs $JOB_ID --output json)

# Extract job queue to determine log group
JOB_QUEUE=$(echo $JOB_DETAILS | jq -r '.jobs[0].jobQueue')

if [[ $JOB_QUEUE == *"cpu"* ]]; then
    LOG_GROUP="/${NAMESPACE}/batch-job/single-node-with-cpu"
elif [[ $JOB_QUEUE == *"gpu"* ]]; then
    LOG_GROUP="/${NAMESPACE}/batch-job/multi-node-with-gpu"
else
    LOG_GROUP="/aws/batch/job"
fi

# Try to get log stream from job attempts
LOG_STREAM=$(echo $JOB_DETAILS | jq -r '.jobs[0].attempts[0].taskProperties.containers[0].logStreamName // empty')

if [ -n "$LOG_STREAM" ] && [ "$LOG_STREAM" != "null" ]; then
    echo "📊 Log Group: $LOG_GROUP"
    echo "📊 Log Stream: $LOG_STREAM"
    
    echo -e "\n📜 Recent Logs:"
    aws logs get-log-events \
      --log-group-name "$LOG_GROUP" \
      --log-stream-name "$LOG_STREAM" \
      --start-from-head \
      --query 'events[*].[timestamp,message]' \
      --output table
else
    echo "⚠️  Job hasn't started yet or log stream not available"
    echo "   Job may be in SUBMITTED/PENDING/RUNNABLE status"
fi

# Monitor job progress
echo -e "\n🔄 Monitoring job progress (Ctrl+C to stop)..."
while true; do
    STATUS=$(aws batch describe-jobs --jobs $JOB_ID --query 'jobs[0].jobStatus' --output text)
    echo "$(date): Job status: $STATUS"
    
    if [[ "$STATUS" == "SUCCEEDED" || "$STATUS" == "FAILED" ]]; then
        echo "Job completed with status: $STATUS"
        break
    fi
    
    sleep 30
done
```

### Quick Log Commands

```bash
# Get the most recent data prep job logs
DATA_PREP_JOB=$(aws batch list-jobs --job-queue $CPU_JOB_QUEUE --job-status RUNNING \
  --query 'jobList[?contains(jobName, `data-prep`)].jobId' --output text | head -1)

if [ -n "$DATA_PREP_JOB" ]; then
    echo "Viewing data prep job logs..."
    aws logs tail /${NAMESPACE}/batch-job/single-node-with-cpu --follow \
      --filter-pattern "job-id=$DATA_PREP_JOB"
fi

# Get the most recent training job logs  
TRAINING_JOB=$(aws batch list-jobs --job-queue $GPU_JOB_QUEUE --job-status RUNNING \
  --query 'jobList[?contains(jobName, `training`)].jobId' --output text | head -1)

if [ -n "$TRAINING_JOB" ]; then
    echo "Viewing training job logs..."
    aws logs tail /${NAMESPACE}/batch-job/multi-node-with-gpu --follow \
      --filter-pattern "job-id=$TRAINING_JOB"
fi
```

### Check S3 Results

```bash
# Check datasets
aws s3 ls s3://$S3_BUCKET/ml-pipeline/datasets/instrument=72089/start_date=2024-01-01/end_date=2024-01-31/

# Check models
aws s3 ls s3://$S3_BUCKET/ml-pipeline/models/instrument=72089/start_date=2024-01-01/end_date=2024-01-31/

# Download training summary
aws s3 cp s3://$S3_BUCKET/ml-pipeline/models/instrument=72089/start_date=2024-01-01/end_date=2024-01-31/training_summary.json ./
```

## ⚙️ Configuration

### batch_config.json

Key configuration parameters:

```json
{
  "data": {
    "metadata_location": "s3://your-data-bucket/metadata/your-table.metadata.json",
    "feature_cols": ["trade_price", "trade_quantity", "bid_price", "ask_price"],
    "instrument_filter": "72089",
    "start_date": "2024-01-01", 
    "end_date": "2024-01-31",
    "lookback_period": 128,
    "apply_undersampling": true
  },
  "s3": {
    "bucket_name": "your-s3-bucket-name",
    "prefix": "ml-pipeline",
    "region": "us-east-1"
  },
  "training": {
    "batch_size": 32,
    "total_epochs": 10,
    "learning_rate": 0.001
  }
}
```

### Infrastructure Parameters

Update `infrastructure/config/parameters.json` if needed:

```json
{
  "batch": {
    "single_node": {
      "container_memory": 16384,
      "container_cpu": 8,
      "instance_classes": ["R5", "R6A", "R7I"]
    },
    "multi_node": {
      "main": {
        "container_memory": 32768,
        "container_gpu": 1,
        "instance_classes": ["G5", "P4D"]
      }
    }
  }
}
```

## 🔧 Performance Optimization

### Memory Optimization
- Use R-series instances for data preparation (memory-intensive)
- Use G-series instances for model training (GPU-accelerated)
- Adjust container memory based on dataset size

### Cost Optimization
- Enable Spot instances: `"spot": true`
- Use appropriate instance families
- Set reasonable job timeouts

### Speed Optimization
- Enable undersampling to reduce dataset size
- Use optimized sequence creation methods
- Leverage S3 Express One Zone for faster I/O

## 🚨 Troubleshooting

### Common Issues

1. **Out of Memory**
   ```
   Solution: Increase container memory or enable undersampling
   ```

2. **S3 Access Denied**
   ```bash
   # Check IAM policies for Batch execution role
   aws iam list-attached-role-policies --role-name AWSBatchServiceRole
   ```

3. **Docker Build Fails**
   ```bash
   # Check dependencies
   docker build --no-cache -t test .
   ```

4. **Job Stuck in RUNNABLE**
   ```
   Solution: Check compute environment capacity and instance availability
   ```

### Debug Commands

```bash
# Check compute environment
aws batch describe-compute-environments

# Check job definition
aws batch describe-job-definitions --job-definition-name $JOB_DEFINITION

# View job logs
aws logs tail /aws/batch/job --follow
```

## 📈 Scaling to Multiple Instruments/Dates

### Batch Processing Multiple Instruments

```bash
# Process multiple instruments in parallel
for instrument in 72089 72090 72091; do
  python scripts/submit_batch_pipeline.py \
    --config configs/batch_config.json \
    --job-queue $JOB_QUEUE \
    --job-definition $JOB_DEFINITION \
    --s3-bucket $S3_BUCKET \
    --instrument $instrument \
    --start-date 2024-01-01 \
    --end-date 2024-01-31 \
    --models CNN_LSTM \
    --no-wait &
done
wait
```

### Time Series Processing

```bash
# Process monthly batches
for month in 01 02 03; do
  python scripts/submit_batch_pipeline.py \
    --config configs/batch_config.json \
    --job-queue $JOB_QUEUE \
    --job-definition $JOB_DEFINITION \
    --s3-bucket $S3_BUCKET \
    --instrument 72089 \
    --start-date 2024-${month}-01 \
    --end-date 2024-${month}-31 \
    --models CNN_LSTM RNN \
    --no-wait &
done
wait
```

## 📋 Next Steps

1. **Model Validation**: Use the trained models with the existing validation scripts
2. **Production Deployment**: Set up scheduled batch jobs for regular retraining
3. **Model Registry**: Implement model versioning and deployment automation
4. **Monitoring**: Set up CloudWatch dashboards for pipeline monitoring
5. **Cost Optimization**: Implement lifecycle policies for S3 data retention

## 🎯 Key Benefits

- **Scalable**: Handles large datasets using AWS Batch auto-scaling
- **Cost-Effective**: Uses Spot instances and optimized resource allocation
- **Structured**: Organized S3 paths for easy data management
- **Reproducible**: Configuration-driven pipeline with version control
- **Monitoring**: Built-in logging and status tracking
- **Flexible**: Support for multiple models and time ranges

## 📚 Data Requirements

### Input Data Format

Your input data should be structured as Iceberg tables with the following columns:

- `trade_price`: Numeric price of trades
- `trade_quantity`: Volume of trades
- `bid_price`: Best bid price
- `ask_price`: Best ask price
- `bid_quantity`: Volume at best bid
- `ask_quantity`: Volume at best ask
- `timestamp`: Time of the data point
- `instrument_id`: Identifier for the financial instrument

### Sample Data Generation

If you don't have existing data, you can generate sample data:

```python
import pandas as pd
import numpy as np
from datetime import datetime, timedelta

# Generate sample financial data
def generate_sample_data(start_date, end_date, instrument_id):
    date_range = pd.date_range(start=start_date, end=end_date, freq='1min')
    
    data = {
        'timestamp': date_range,
        'instrument_id': instrument_id,
        'trade_price': np.random.normal(100, 10, len(date_range)),
        'trade_quantity': np.random.exponential(100, len(date_range)),
        'bid_price': np.random.normal(99.5, 10, len(date_range)),
        'ask_price': np.random.normal(100.5, 10, len(date_range)),
        'bid_quantity': np.random.exponential(50, len(date_range)),
        'ask_quantity': np.random.exponential(50, len(date_range))
    }
    
    return pd.DataFrame(data)

# Generate and save sample data
sample_df = generate_sample_data('2024-01-01', '2024-01-31', '72089')
sample_df.to_parquet('sample_data.parquet')
```

---

**Built with ❤️ for quantitative researchers**
