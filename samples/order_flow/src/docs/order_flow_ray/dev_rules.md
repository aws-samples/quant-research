# Development Rules and Guidelines

## Python Environment

### Virtual Environment
- **Required Python**: Use Python 3.10 virtual environment
- **Path**: `/opt/homebrew/anaconda3/envs/python_3_10/bin/python3`
- **Activation**: `source /opt/homebrew/anaconda3/bin/activate && conda activate python_3_10`
- **Usage**: All scripts must be run with this Python interpreter
- **Example**: `/opt/homebrew/anaconda3/envs/python_3_10/bin/python3 script_name.py`

### Path Configuration (REQUIRED)
- **Rule**: All Python scripts and notebooks MUST include this path setup at the top:
  ```python
  import sys
  import os
  
  current_dir = os.getcwd()
  if 'data_preprocessing' in current_dir:
      src_dir = os.path.dirname(current_dir)
  else:
      src_dir = os.path.join(current_dir, 'samples', 'order_flow_ray', 'src')
  sys.path.append(src_dir)
  ```
- **Purpose**: Ensures imports work correctly from any location (notebooks, tests, scripts)
- **Usage**: Place immediately after standard library imports, before project imports

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

### Midway Authentication (REQUIRED)
- **Critical**: Run `mwinit -s` at the start of every coding session
- **Purpose**: Authenticates with AWS internal GitLab and signs SSH keys
- **Frequency**: Required daily or when session expires
- **Command**: `mwinit -s` (follow prompts for PIN and security key)
- **Verification**: Should see "Successfully signed SSH public key" message

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

## S3 Tables and Iceberg

### Partitioning Strategy (REQUIRED)
- **Rule**: All S3 Tables MUST be partitioned using 6-level structure matching BMLL S3 path
- **Purpose**: Prevents commit conflicts when multiple Ray workers write simultaneously
- **Partition Columns** (in order):
  1. `EventDate` (level2q) or `TradeDate` (trades) or `Date` (reference): Date column, use year transform
  2. `EventDate` (level2q) or `TradeDate` (trades) or `Date` (reference): Date column, use month transform
  3. `EventDate` (level2q) or `TradeDate` (trades) or `Date` (reference): Date column, use day transform
  4. `DataType`: Data type identifier (trades, level2q, reference), use identity transform
  5. `Region`: Region identifier (AMERICAS), use identity transform
  6. `ISOExchangeCode`: Exchange identifier (ARCX, XNYS, etc.), use identity transform
- **Implementation**: Pass appropriate date column based on data_type to partition_by in S3Tables write operations
- **Note**: Year/month/day transforms are applied automatically by PyIceberg on the date column
- **Rationale**: Matches BMLL S3 structure `YYYY/MM/DD/{data_type}/AMERICAS/{exchange}`, ensuring each worker writes to unique partition

## Future Rules

Additional rules will be documented here as the project evolves.