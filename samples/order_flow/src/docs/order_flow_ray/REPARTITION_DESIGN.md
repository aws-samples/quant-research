# Repartition Step Design Document

## Overview
Add a flexible repartition step that can be inserted at any point in the pipeline to reorganize data by adding a partition key (e.g., Ticker) to the S3 path structure for improved data locality and query performance.

## Purpose
Repartitioning enables:
- **Data Locality**: Co-locate related data (e.g., all SPY data) for faster queries
- **Parallel Processing**: Process partitions independently in downstream steps
- **Query Optimization**: Filter by partition key without scanning all files
- **Flexibility**: Can be applied after any pipeline step (normalization, feature engineering, etc.)

## Path Structure Transformation

**Generic Pattern:**
```
Input:  s3://bucket/stage/input/YYYY/MM/DD/data_type/region/exchange-YYYYMMDD.parquet
Output: s3://bucket/stage/output/YYYY/MM/DD/data_type/region/{partition_key}/exchange-YYYYMMDD.parquet
```

**Example Use Cases:**

1. **After Normalization** (partition by Ticker):
```
Input:  s3://bucket/intermediate/normalized/2024/12/31/trades/AMERICAS/trades-XNAS-20241231.parquet
Output: s3://bucket/intermediate/repartitioned/2024/12/31/trades/AMERICAS/SPY/trades-XNAS-20241231.parquet
```

2. **After Feature Engineering** (partition by Ticker):
```
Input:  s3://bucket/intermediate/features/2024/12/31/trades/AMERICAS/features-XNAS-20241231.parquet
Output: s3://bucket/intermediate/features_by_ticker/2024/12/31/trades/AMERICAS/SPY/features-XNAS-20241231.parquet
```

3. **After Training** (partition by Model):
```
Input:  s3://bucket/models/2024/12/31/predictions/predictions-20241231.parquet
Output: s3://bucket/models/2024/12/31/predictions/model_v1/predictions-20241231.parquet
```

## Reusable Patterns from Existing Implementation

### From Normalization (`pipeline.py::_run_normalization`)
1. **Ray Remote Functions**: Use `@ray.remote(num_cpus=X, max_retries=0)` for distributed processing
2. **Dynamic Resource Allocation**: Calculate memory/CPU based on file size with `memory_multiplier` and `memory_per_core_gb`
3. **Result Dictionary Format**: Return standardized dict with `{file, size_gb, memory_gb, cpus, input_path, output_path, row_count, data_type, stage, message}`
4. **Data Access Factory**: Use `DataAccessFactory.create('s3', region, profile_name)` for S3 operations
5. **Path Extraction**: Parse `data_type` from path using `parts = fp.split('/')` and `data_type = parts[-3]`
6. **Error Handling**: Wrap in try/except, return error dict with same structure but `message=str(e)`

### From Feature Engineering (`order_flow.py::discover_files`)
1. **File Discovery**: Use `data_access.list_files(input_path)` to get all files with sizes
2. **Path Parsing**: Extract components using `parts = file_path.split('/')` and skip S3 prefix (`parts[5:]`)
3. **File Grouping**: Group by `(yyyy, mm, dd, region, exchange)` using `defaultdict(list)`
4. **Sorting**: Sort groups and files within groups based on `file_sort_order` parameter
5. **Return Format**: Return `List[List[tuple[str, int]]]` for grouped processing

### From Pipeline (`pipeline.py::_execute_step_with_retry`)
1. **Retry Logic**: Use `max_retries` attribute from step instance
2. **Failure Extraction**: Call `step_instance.get_failed_items(results)` to get items for retry
3. **Progress Reporting**: Print attempt details with success/failure counts
4. **Result Aggregation**: Collect all results across retries in `all_results` list

## Implementation Components

### 1. Repartition Class (`data_preprocessing/repartition.py`)
```python
class Repartition:
    def __init__(self, partition_column: str = 'Ticker', max_retries: int = 3):
        """Initialize repartition with partition column and retry configuration.
        
        Args:
            partition_column: Column name to partition by (e.g., 'Ticker', 'Model', 'Strategy')
            max_retries: Maximum retry attempts for failed files
        """
        self.partition_column = partition_column
        self.max_retries = max_retries
    
    def discover_files(self, data_access, input_path: str, file_sort_order: str) -> List[tuple[str, int]]:
        """Discover parquet files for repartitioning from any pipeline stage.
        
        Args:
            data_access: Data access instance
            input_path: S3 path to input data (can be normalized, features, etc.)
            file_sort_order: Sort order ('asc' or 'desc')
        
        Returns:
            List of (file_path, size) tuples sorted by file_sort_order
        """
        pass
    
    def repartition(self, df: pl.LazyFrame, source_path: str) -> pl.LazyFrame:
        """Repartition data - returns data as-is (actual partitioning happens in write).
        
        Args:
            df: Input data from any pipeline stage
            source_path: Source file path for logging
            
        Returns:
            Same LazyFrame (partitioning happens during write by partition_column)
        """
        pass
    
    def get_failed_items(self, results: List[Any]) -> List[tuple[str, int]]:
        """Extract failed items for retry."""
        pass
    
    def failure_extraction(self, results: List[Any]) -> List[Any]:
        """Extract failure results."""
        pass
```

### 2. Pipeline Integration (`pipeline/pipeline.py`)

#### Add Repartition Step Method
```python
def _run_repartition(self, files: List[tuple[str, int]]) -> list[dict]:
    """Run repartition for given files.
    
    For each input file:
    1. Read normalized data
    2. Group by Ticker
    3. Write each ticker group to separate file with ticker in path
    4. Return result dict for each ticker file written
    """
    pass

def _repartition_step(self, data: List[tuple[str, int]]) -> Any:
    """Execute repartition step with retry logic."""
    return self._execute_step_with_retry(
        'repartition',
        data,
        self.config.processing.repartition,
        self._run_repartition
    )
```

#### Update Pipeline Run Method
```python
def run(self, files_slice=slice(None), specific_files: list[str] | None = None):
    """Execute pipeline steps based on configuration.
    
    Repartition can be inserted at any point in the pipeline by configuring
    the appropriate input/output storage locations.
    """
    # ... existing code ...
    
    # Example 1: Repartition after normalization
    if self.config.processing.normalization:
        print("Running normalization...")
        data = self._normalize_step(data)
    
    if self.config.processing.repartition:
        print("Running repartition...")
        data = self._repartition_step(data)
    
    if self.config.processing.feature_engineering:
        print("Running feature engineering...")
        data = self._feature_engineering_step(data)
    
    # Example 2: Repartition after feature engineering
    # if self.config.processing.feature_engineering:
    #     print("Running feature engineering...")
    #     data = self._feature_engineering_step(data)
    # 
    # if self.config.processing.repartition:
    #     print("Running repartition...")
    #     data = self._repartition_step(data)
    
    # ... rest of pipeline ...
```

### 3. Configuration (`pipeline/config.py`)

#### Add to ProcessingConfig
```python
@dataclass
class ProcessingConfig:
    normalization: Optional[Normalization] = None
    repartition: Optional[Repartition] = None  # NEW
    feature_engineering: Optional[FeatureEngineering] = None
    training: Optional[Training] = None
    inference: Optional[Inference] = None
    backtest: Optional[Backtest] = None
```

#### Add to StorageConfig
```python
@dataclass
class StorageConfig:
    raw: StorageLocation
    normalized: StorageLocation
    repartitioned: StorageLocation  # NEW
    features: StorageLocation
    models: StorageLocation
    predictions: StorageLocation
    backtest_results: StorageLocation
```

## Processing Logic

### File Discovery
- **Input**: Configurable S3 path (e.g., `s3://bucket/intermediate/normalized` or `s3://bucket/intermediate/features`)
- List all `.parquet` files using `data_access.list_files()`
- Filter for `.parquet` extension
- No grouping needed (process files individually)
- Sort by `file_sort_order` (asc/desc)
- Return `List[tuple[str, int]]` format
- **Flexible**: Works with any pipeline stage output

### Repartition Processing (Per File)

#### Ray Remote Function
```python
@ray.remote(num_cpus=num_cpus, max_retries=0)
def repartition_file(fp: str, fs: float, region: str, input_base: str, 
                     output_loc_dict: dict, partition_col: str, mem_gb: float, cpus: int, profile: str) -> List[dict]:
    """Repartition a single file by partition column (flexible for any pipeline stage).
    
    Args:
        partition_col: Column name to partition by (e.g., 'Ticker', 'Model', 'Strategy')
    
    Returns:
        List of result dicts (one per partition value written)
    """
    results = []
    
    try:
        # 1. Read input data (works with any pipeline stage output)
        data_access = DataAccessFactory.create('s3', region=region, profile_name=profile)
        df = data_access.read(fp)
        
        # 2. Extract path components
        parts = fp.split('/')
        yyyy, mm, dd, data_type, region_name = parts[-5:-1]
        filename = parts[-1]
        
        # 3. Get unique partition values
        partition_values = df.select(partition_col).unique().collect()[partition_col].to_list()
        
        # 4. Process each partition value
        for partition_value in partition_values:
            partition_df = df.filter(pl.col(partition_col) == partition_value)
            
            # Construct output path with partition value
            output_path = f"{output_loc_dict['path']}/{yyyy}/{mm}/{dd}/{data_type}/{region_name}/{partition_value}/{filename}"
            
            # Write partition-specific file
            data_access.write(partition_df, output_path)
            
            # Get row count and file size
            row_count = partition_df.select(pl.len()).collect().item()
            output_size_mb = data_access.get_file_size(output_path) / (1024 ** 2)
            
            results.append({
                'file': filename,
                'size_gb': fs,
                'memory_gb': mem_gb,
                'cpus': cpus,
                'input_path': fp,
                'output_path': output_path,
                'row_count': row_count,
                'output_size_mb': output_size_mb,
                'partition_column': partition_col,
                'partition_value': partition_value,
                'data_type': data_type,
                'stage': 'repartition',
                'message': 'success'
            })
    
    except Exception as e:
        results.append({
            'file': fp.split('/')[-1],
            'size_gb': fs,
            'memory_gb': mem_gb,
            'cpus': cpus,
            'input_path': fp,
            'output_path': None,
            'row_count': None,
            'output_size_mb': 0,
            'partition_column': partition_col,
            'partition_value': None,
            'data_type': parts[-3] if len(parts := fp.split('/')) >= 3 else 'unknown',
            'stage': 'repartition',
            'message': str(e)
        })
    
    return results
```

### Output Path Construction
```python
# Input: s3://bucket/intermediate/normalized/2024/12/31/trades/AMERICAS/trades-XNAS-20241231.parquet
# Parse: yyyy=2024, mm=12, dd=31, data_type=trades, region=AMERICAS, filename=trades-XNAS-20241231.parquet

# For ticker='SPY':
# Output: s3://bucket/intermediate/repartitioned/2024/12/31/trades/AMERICAS/SPY/trades-XNAS-20241231.parquet

# For ticker='AAPL':
# Output: s3://bucket/intermediate/repartitioned/2024/12/31/trades/AMERICAS/AAPL/trades-XNAS-20241231.parquet
```

### Resource Allocation
```python
if config.ray.flat_core_count is not None:
    num_cpus = config.ray.flat_core_count
    memory_gb = num_cpus * config.ray.memory_per_core_gb
else:
    memory_bytes = int(file_size * memory_multiplier)
    memory_gb = memory_bytes / (1024 ** 3)
    num_cpus = ceil(memory_gb / config.ray.memory_per_core_gb) + config.ray.cpu_buffer
```

### Result Dictionary Format
```python
{
    'file': 'trades-XNAS-20241231.parquet',
    'size_gb': 2.5,
    'memory_gb': 5.0,
    'cpus': 4,
    'input_path': 's3://bucket/intermediate/normalized/2024/12/31/trades/AMERICAS/trades-XNAS-20241231.parquet',
    'output_path': 's3://bucket/intermediate/repartitioned/2024/12/31/trades/AMERICAS/SPY/trades-XNAS-20241231.parquet',
    'row_count': 150000,
    'output_size_mb': 45.2,
    'ticker': 'SPY',
    'data_type': 'trades',
    'stage': 'repartition',
    'message': 'success'
}
```

## Key Considerations

### 1. Multiple Outputs Per Input
- One input file produces multiple output files (one per ticker)
- Ray remote function returns `List[dict]` instead of single dict
- Pipeline flattens results: `results = []; for group_result in ray.get(futures): results.extend(group_result)`

### 2. Memory Management
- Input file size may not reflect output size distribution
- Allocate based on input file size (conservative approach)
- Monitor actual memory usage per ticker group
- Consider ticker cardinality when sizing resources

### 3. Path Consistency
- Maintain same structure as normalized, just add ticker level
- Preserves date/data_type/region hierarchy
- Enables efficient querying by ticker
- Format: `{base}/{yyyy}/{mm}/{dd}/{data_type}/{region}/{ticker}/{filename}`

### 4. Retry Logic
- Failed files should retry entire file, not individual tickers
- Use `get_failed_items()` to extract failed input files based on `input_path`
- Re-process entire file on retry (all tickers)
- Track failures at input file level, not ticker level

### 5. Data Validation
- Ensure partition column exists in input data (flexible: Ticker, Model, Strategy, etc.)
- If missing, log error and fail with descriptive message
- Do not attempt to infer partition value from filename
- Partition column must be present in data schema

### 6. Performance Optimization
- Repartitioning is I/O intensive (read once, write N times)
- Use Ray parallelism to process multiple files concurrently
- Consider batching small files if ticker cardinality is low
- Monitor S3 write throughput and adjust parallelism

### 7. Error Handling
- Wrap entire processing in try/except
- Return error result dict with `message=str(e)`
- Include partial results if some tickers succeed before failure
- Log detailed error information for debugging

### 8. Progress Reporting
- Report at file level: "Processing file X of Y"
- Report ticker count per file: "File contains N tickers"
- Report success/failure per ticker: "Wrote ticker SPY: 150K rows, 45MB"
- Final summary: "Processed X files, wrote Y ticker files, Z failures"

## Configuration Example

```yaml
processing:
  normalization:
    max_retries: 3
  
  repartition:
    max_retries: 3
  
  feature_engineering:
    bar_duration_ms: 250
    max_retries: 3

storage:
  normalized:
    path: s3://orderflowanalysis/intermediate/normalized
  
  repartitioned:
    path: s3://orderflowanalysis/intermediate/repartitioned
  
  features:
    path: s3://orderflowanalysis/intermediate/features

ray:
  memory_multiplier: 2.0
  memory_per_core_gb: 4.0
  cpu_buffer: 1
  file_sort_order: desc
```

## Testing Strategy

### Unit Tests
1. Test path parsing and construction
2. Test partition value extraction from data (Ticker, Model, etc.)
3. Test result dict format
4. Test error handling for missing partition column

### Integration Tests
1. Test single file with single ticker
2. Test single file with multiple tickers
3. Test multiple files in parallel
4. Test retry logic with simulated failures
5. Test memory allocation for various file sizes

### End-to-End Tests
1. Run full pipeline: normalization → repartition → feature engineering
2. Verify output path structure
3. Verify data integrity (row counts match)
4. Verify all tickers present in output
5. Measure performance and resource usage
