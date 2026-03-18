# Order Flow Ray Pipeline Architecture

## Overview

Ray-based distributed processing pipeline for BMLL order flow data, designed for scalable feature engineering and ML model training.

## Component Architecture

### 1. Data Engineering (`data_engineering/`)

**Purpose**: Distributed data loading, validation, and preprocessing

**Components**:
- **File Discovery**: Ray tasks to discover and catalog BMLL files across date ranges
- **Data Loaders**: Parallel S3 parquet file loading with polars
- **Data Validators**: Schema validation and data quality checks
- **Data Joiners**: Temporal joins between trades, level2q, and reference data

**Key Ray Tasks**:
```python
@ray.remote
def discover_files(date_range, exchanges, data_types) -> List[FileMetadata]

@ray.remote  
def load_bmll_file(s3_path, storage_options) -> pl.DataFrame

@ray.remote
def validate_data_quality(df, schema_config) -> ValidationReport

@ray.remote
def join_market_data(trades_df, level2q_df, reference_df) -> pl.DataFrame
```

**Parallelization Strategy**:
- File discovery: Parallel S3 listing by date
- Data loading: One Ray task per file (exchange-date combination)
- Validation: Parallel validation across loaded DataFrames
- Joining: Parallel joins within each exchange-date group

### 2. Feature Engineering (`feature_engineering/`)

**Purpose**: Distributed computation of order flow and microstructure features

**Components**:
- **Order Flow Features**: Level2 book imbalance, order flow toxicity, trade aggressiveness
- **Rolling Statistics**: Time-windowed aggregations of price and volume
- **Cross-Exchange Features**: Inter-exchange spread and arbitrage signals
- **Microstructure Features**: Bid-ask dynamics, trade clustering, market impact

**Key Ray Tasks**:
```python
@ray.remote
def compute_all_features(trades_df, level2q_df, reference_df, config) -> pl.DataFrame

@ray.remote
def compute_order_flow_features(trades_df, level2q_df, config) -> pl.DataFrame

@ray.remote
def compute_rolling_features(df, window_configs) -> pl.DataFrame

@ray.remote
def compute_cross_exchange_features(exchange_dfs) -> pl.DataFrame
```

**Feature Categories**:
- **Order Flow**: Book imbalance, trade direction, flow toxicity
- **Level2 Features**: Bid-ask spread, depth, order count imbalances
- **Microstructure**: Trade clustering, market impact, execution quality
- **Rolling Stats**: Price/volume rolling means, std, quantiles
- **Cross-Market**: Inter-exchange spreads, lead-lag relationships

### 3. Utilities (`util/`)

**Purpose**: Shared utilities and helper functions

**Components**:
- **AWS Helpers**: S3 credential management, bucket operations
- **Ray Helpers**: Cluster management, task monitoring, error handling
- **Data Helpers**: Schema utilities, data type conversions
- **Config Management**: Pipeline configuration and parameter management

**Key Utilities**:
```python
# AWS utilities
def get_aws_credentials(profile_name) -> Dict
def setup_s3_storage_options(credentials) -> Dict

# Ray utilities  
def initialize_ray_cluster(config) -> None
def monitor_ray_tasks(task_refs) -> TaskStatus
def handle_ray_failures(failed_tasks) -> None

# Data utilities
def standardize_bmll_schema(df, data_type) -> pl.DataFrame
def validate_join_keys(df1, df2, keys) -> bool
```

### 4. ML Pipeline (`ml_pipeline/`)

**Purpose**: Distributed model training and evaluation

**Components**:
- **Dataset Preparation**: Sequence creation, temporal splitting, undersampling
- **Feature Scaling**: Distributed MinMax scaling with Ray
- **Model Training**: Distributed hyperparameter tuning
- **Model Evaluation**: Parallel model evaluation across test sets

**Key Ray Tasks**:
```python
@ray.remote
def create_sequences(df, sequence_length, target_col) -> SequenceDataset

@ray.remote
def compute_scaling_params(df, feature_cols) -> ScalingParams

@ray.remote
def train_model(model_config, train_data, val_data) -> TrainedModel

@ray.remote
def evaluate_model(model, test_data) -> EvaluationMetrics
```

## Data Flow Architecture

```
Input: Date Range + Instruments + Exchanges
    ↓
[Data Engineering]
├── File Discovery (Ray Tasks)
├── Parallel Data Loading (Ray Tasks)  
├── Data Validation (Ray Tasks)
└── Temporal Joins (Ray Tasks)
    ↓
[Feature Engineering] 
├── Order Flow Features (Ray Tasks)
├── Rolling Statistics (Ray Tasks)
└── Cross-Exchange Features (Ray Tasks)
    ↓
[ML Pipeline]
├── Dataset Preparation (Ray Tasks)
├── Feature Scaling (Ray Tasks)
├── Model Training (Ray Tasks)
└── Model Evaluation (Ray Tasks)
    ↓
Output: Trained Models + Evaluation Metrics
```

## Parallelization Levels

### Level 1: Date-Level Parallelism
- Process multiple dates simultaneously
- Each date spawns exchange-level tasks

### Level 2: Exchange-Level Parallelism  
- Process multiple exchanges per date simultaneously
- Each exchange-date combination processes all features together
- Single Ray task per exchange-date handles complete feature engineering

### Level 3: Model-Level Parallelism
- Train multiple models simultaneously after feature engineering complete
- Hyperparameter tuning across parameter space
- Parallel model evaluation across different architectures

## Resource Management

### Memory Strategy
- **Per-Task Memory**: 4-8GB (handles typical exchange-date files)
- **Streaming Processing**: Use polars lazy evaluation within tasks
- **Result Chunking**: Return processed features, not raw data
- **Garbage Collection**: Explicit cleanup after each task

### CPU Strategy  
- **Per-Task CPUs**: 2-4 cores for polars operations
- **Task Granularity**: Exchange-date level (optimal for parallelism)
- **Load Balancing**: Ray's automatic task distribution

### Storage Strategy
- **Input**: Direct S3 access with parallel downloads
- **Intermediate**: Ray object store for feature DataFrames
- **Output**: S3 for final datasets and models
- **Caching**: Ray object store for reused reference data

## Fault Tolerance

### Task-Level Resilience
- **Stateless Tasks**: Each Ray task is independent and retryable
- **Automatic Retries**: Ray handles task failures with exponential backoff
- **Partial Results**: Continue processing even if some tasks fail

### Data-Level Resilience  
- **Validation Gates**: Data quality checks before feature engineering
- **Schema Enforcement**: Strict schema validation for all data types
- **Graceful Degradation**: Skip problematic files, continue with available data

### Pipeline-Level Resilience
- **Checkpointing**: Save intermediate results to S3 for large jobs
- **Resume Capability**: Restart from last successful checkpoint
- **Monitoring**: Real-time task monitoring and alerting

## Configuration Management

### Pipeline Configuration
```yaml
ray_config:
  max_workers: 20
  memory_per_worker: "8GB" 
  cpu_per_worker: 4
  
data_config:
  date_range: ["2024-01-01", "2024-01-31"]
  exchanges: ["ARCX", "BATS", "EDGA"]
  instruments: ["AAPL", "MSFT", "GOOGL"]
  
feature_config:
  rolling_windows: [5, 10, 20]
  order_flow_features: ["book_imbalance", "trade_toxicity", "trade_aggressiveness"]
  microstructure_features: ["bid_ask_dynamics", "trade_clustering"]
  cross_exchange_features: ["price_spreads", "lead_lag"]
  
ml_config:
  sequence_length: 128
  train_split: 0.7
  val_split: 0.15
  models: ["CNN_LSTM", "TFT"]
```

This architecture provides scalable, fault-tolerant processing of BMLL data with Date → Exchange → Model parallelization, focusing on order flow and microstructure features without traditional technical indicators.