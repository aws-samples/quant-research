# Development Rules and Guidelines

## Python Environment

### Virtual Environment
- **Required Python**: Use Python 3.10 virtual environment
- **Path**: `/opt/homebrew/anaconda3/envs/python_3_10/bin/python3`
- **Activation**: `source /opt/homebrew/anaconda3/bin/activate && conda activate python_3_10`
- **Usage**: All scripts must be run with this Python interpreter
- **Example**: `/opt/homebrew/anaconda3/envs/python_3_10/bin/python3 script_name.py`

## Project Structure Rules

### Working Directories

1. **Ray Pipeline Development**
   - **Primary Working Directory**: `/Users/blitvin/IdeaProjects/quant-research-sample-using-amazon-ecs-and-aws-batch/samples/order_flow_ray/src`
   - **Purpose**: All Ray-based pipeline development and production code
   - **Usage**: New components, data preprocessing, feature engineering, ML pipeline

### File Organization

1. **Ad-hoc Exploration Scripts**
   - **Rule**: All ad-hoc exploration scripts MUST be placed in `src/util/scratch/`
   - **Purpose**: Keep experimental and one-off analysis scripts separate from production code
   - **Examples**: Data exploration, bucket analysis, schema discovery scripts
   - **Location**: `/samples/order_flow/src/util/scratch/`

### Directory Structure

```
src/
├── docs/                    # Documentation and rules
├── util/                    # Utility functions and tools
│   └── scratch/            # Ad-hoc exploration scripts (REQUIRED LOCATION)
├── batch_*.py              # Main pipeline components
├── *.py                    # Core pipeline modules
└── ...
```

## Context Reset Recovery

When context is reset, refer to this document to understand:
- Python environment path and requirements
- Where to place new exploration scripts
- Project organization standards
- Development workflow rules

## AWS and S3 Access

### Polars S3 Configuration
- **Issue**: Polars requires explicit AWS credentials for S3 access
- **Solution**: Pass credentials via `storage_options` parameter
- **Required Code Pattern**:
  ```python
  import boto3
  import polars as pl
  
  # Get credentials from boto3 session
  session = boto3.Session(profile_name='blitvinfdp')
  credentials = session.get_credentials()
  
  # Configure storage options for polars
  storage_options = {
      "aws_region": "us-east-1",
      "aws_access_key_id": credentials.access_key,
      "aws_secret_access_key": credentials.secret_key
  }
  
  if credentials.token:
      storage_options["aws_session_token"] = credentials.token
  
  # Use with polars
  df = pl.scan_parquet(s3_path, storage_options=storage_options)
  ```
- **Note**: Standard AWS profile configuration alone is insufficient for polars

## Future Rules

Additional rules will be documented here as the project evolves.