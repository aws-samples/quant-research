# Component Specifications

## 1. Data Engineering Components

### File Discovery Service
**Responsibility**: Discover and catalog BMLL files across S3 buckets

**Input**: Date range, exchanges list, data types
**Output**: List of FileMetadata objects with S3 paths, sizes, schemas

**Ray Task Design**:
```python
@ray.remote
class FileDiscoveryActor:
    def discover_date_range(self, start_date, end_date, exchanges, data_types)
    def validate_file_availability(self, file_list)
    def estimate_processing_resources(self, file_list)
```

**Key Features**:
- Parallel S3 listing across date ranges
- File size estimation for resource planning
- Missing file detection and reporting
- Duplicate file handling

### Data Loader Service
**Responsibility**: Load and standardize BMLL parquet files

**Input**: S3 file paths with storage options
**Output**: Standardized polars DataFrames

**Ray Task Design**:
```python
@ray.remote
def load_trades_file(s3_path, storage_options, schema_config)
def load_level2q_file(s3_path, storage_options, schema_config)  
def load_reference_file(s3_path, storage_options, schema_config)
```

**Key Features**:
- Schema standardization across data types
- Data type enforcement and conversion
- Memory-efficient streaming with polars
- Error handling for corrupted files

### Data Validator Service
**Responsibility**: Validate data quality and schema compliance

**Input**: Raw DataFrames from loaders
**Output**: ValidationReport with quality metrics

**Ray Task Design**:
```python
@ray.remote
def validate_schema_compliance(df, expected_schema)
def validate_data_quality(df, quality_rules)
def validate_temporal_consistency(df, time_column)
```

**Validation Rules**:
- Schema compliance (column names, types)
- Data completeness (null value thresholds)
- Temporal consistency (timestamp ordering)
- Value range validation (price/quantity bounds)

### Data Joiner Service  
**Responsibility**: Join trades, level2q, and reference data

**Input**: Validated DataFrames for same exchange-date
**Output**: Joined DataFrame with market data

**Ray Task Design**:
```python
@ray.remote
def join_market_data(trades_df, level2q_df, reference_df, join_config)
def temporal_align_data(df1, df2, time_tolerance)
def enrich_with_reference(market_df, reference_df)
```

**Join Strategy**:
- Temporal joins with configurable tolerance
- As-of joins for reference data enrichment
- Instrument-level joins using Ticker/InstrumentId
- Memory-efficient join algorithms

## 2. Feature Engineering Components

### Technical Indicators Engine
**Responsibility**: Compute price and volume-based technical indicators

**Input**: Market data DataFrame with OHLCV
**Output**: DataFrame with technical indicator columns

**Ray Task Design**:
```python
@ray.remote
def compute_price_indicators(df, indicator_configs)
def compute_volume_indicators(df, indicator_configs)
def compute_volatility_indicators(df, indicator_configs)
```

**Indicator Categories**:
- **Trend**: SMA, EMA, MACD, Bollinger Bands
- **Momentum**: RSI, Stochastic, Williams %R
- **Volume**: VWAP, OBV, Volume Profile
- **Volatility**: ATR, Realized Volatility, GARCH

### Order Flow Features Engine
**Responsibility**: Compute microstructure and order flow features

**Input**: Trades and Level2Q DataFrames
**Output**: DataFrame with order flow features

**Ray Task Design**:
```python
@ray.remote
def compute_book_imbalance(level2q_df, config)
def compute_trade_toxicity(trades_df, level2q_df, config)
def compute_order_flow_metrics(trades_df, config)
```

**Feature Categories**:
- **Book Features**: Bid-ask spread, depth, imbalance ratios
- **Trade Features**: Trade size, aggressiveness, clustering
- **Flow Features**: Order flow toxicity, VPIN, trade direction
- **Timing Features**: Inter-arrival times, execution delays

### Rolling Statistics Engine
**Responsibility**: Compute time-windowed aggregations and statistics

**Input**: Feature DataFrame with timestamps
**Output**: DataFrame with rolling statistics

**Ray Task Design**:
```python
@ray.remote
def compute_rolling_aggregates(df, window_configs)
def compute_rolling_correlations(df, feature_pairs, windows)
def compute_rolling_quantiles(df, quantile_configs)
```

**Rolling Features**:
- **Aggregates**: Mean, std, min, max, sum
- **Quantiles**: 5th, 25th, 50th, 75th, 95th percentiles
- **Correlations**: Cross-feature correlations
- **Custom**: Skewness, kurtosis, entropy

### Cross-Exchange Features Engine
**Responsibility**: Compute inter-exchange relationships and arbitrage signals

**Input**: Multiple exchange DataFrames for same instruments
**Output**: DataFrame with cross-exchange features

**Ray Task Design**:
```python
@ray.remote
def compute_price_spreads(exchange_dfs, instrument_filter)
def compute_lead_lag_relationships(exchange_dfs, config)
def compute_arbitrage_signals(exchange_dfs, config)
```

**Cross-Exchange Features**:
- **Spreads**: Price differences across exchanges
- **Lead-Lag**: Cross-correlation with time lags
- **Arbitrage**: Statistical arbitrage opportunities
- **Flow**: Cross-exchange order flow patterns

## 3. Utility Components

### AWS Integration Utilities
**Responsibility**: Handle AWS services integration

**Functions**:
```python
def setup_aws_credentials(profile_name="blitvinfdp")
def create_s3_storage_options(credentials, region="us-east-1")
def upload_results_to_s3(data, bucket, key)
def download_config_from_s3(bucket, key)
```

### Ray Cluster Management
**Responsibility**: Manage Ray cluster lifecycle and monitoring

**Functions**:
```python
def initialize_ray_cluster(config)
def monitor_task_progress(task_refs)
def handle_task_failures(failed_tasks, retry_config)
def cleanup_ray_resources()
```

### Data Schema Utilities
**Responsibility**: Handle data schema operations

**Functions**:
```python
def load_bmll_schemas() -> Dict[str, Schema]
def validate_dataframe_schema(df, schema)
def standardize_column_names(df, data_type)
def convert_data_types(df, type_mapping)
```

### Configuration Management
**Responsibility**: Handle pipeline configuration

**Functions**:
```python
def load_pipeline_config(config_path)
def validate_config_parameters(config)
def merge_config_overrides(base_config, overrides)
def save_execution_config(config, output_path)
```

## 4. ML Pipeline Components

### Dataset Preparation Service
**Responsibility**: Create ML-ready datasets from features

**Input**: Feature DataFrames with targets
**Output**: Train/Val/Test sequence datasets

**Ray Task Design**:
```python
@ray.remote
def create_sequence_dataset(df, sequence_length, target_col)
def temporal_split_data(df, split_ratios)
def apply_undersampling(dataset, strategy)
```

**Preparation Steps**:
- Sequence creation with sliding windows
- Temporal train/validation/test splits
- Class balancing and undersampling
- Data leakage prevention

### Feature Scaling Service
**Responsibility**: Compute and apply feature scaling

**Input**: Training features DataFrame
**Output**: Scaling parameters and scaled datasets

**Ray Task Design**:
```python
@ray.remote
def compute_scaling_parameters(train_df, feature_cols)
def apply_feature_scaling(df, scaling_params)
def validate_scaling_quality(scaled_df, original_df)
```

**Scaling Methods**:
- MinMax scaling (0-1 normalization)
- StandardScaler (z-score normalization)  
- RobustScaler (median-based scaling)
- Custom scaling for financial features

### Model Training Service
**Responsibility**: Distributed model training and hyperparameter tuning

**Input**: Prepared datasets and model configurations
**Output**: Trained models with validation metrics

**Ray Task Design**:
```python
@ray.remote
def train_single_model(model_config, train_data, val_data)
def hyperparameter_search(model_type, param_grid, data)
def cross_validate_model(model_config, data, cv_folds)
```

**Training Features**:
- Parallel model training across configurations
- Distributed hyperparameter optimization
- Cross-validation with temporal awareness
- Early stopping and model checkpointing

### Model Evaluation Service
**Responsibility**: Comprehensive model evaluation and comparison

**Input**: Trained models and test datasets
**Output**: Evaluation metrics and performance reports

**Ray Task Design**:
```python
@ray.remote
def evaluate_classification_model(model, test_data)
def compute_financial_metrics(predictions, returns, costs)
def generate_evaluation_report(metrics, model_info)
```

**Evaluation Metrics**:
- **Classification**: Accuracy, F1, Precision, Recall, AUC
- **Financial**: Sharpe ratio, Max drawdown, Hit rate
- **Risk**: VaR, Expected shortfall, Beta
- **Operational**: Latency, Throughput, Resource usage

This component specification provides the detailed design for each service in the Ray-based order flow pipeline, ensuring scalable and maintainable distributed processing.