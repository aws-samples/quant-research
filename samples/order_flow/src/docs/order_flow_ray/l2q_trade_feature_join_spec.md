# L2Q + Trade Feature Join Implementation Specification

## Overview

This document outlines the implementation plan for joining L2Q (Level 2 Quote) and Trade features into a unified dataset. The join step runs after individual feature engineering and combines features using full outer join with forward fill for missing data.

## Architecture

### Processing Flow
```
L2Q Features     Trade Features
     |                |
     v                v
  Discover Files  Discover Files
     |                |
     v                v
     +-- File Pairing --+
            |
            v
     Group File Pairs
            |
            v
      Join Processing
            |
            v
   Combined Features Output
```

## Implementation Plan

### 1. Data Access Layer Enhancement
**File:** `/data_preprocessing/data_access/s3.py`

Add convenience method for file discovery with sorting:
```python
def discover_files_asynch(self, path: str, sort_order: str = 'asc', parallel_discovery_threshold: int = 100) -> list[tuple[str, int]]:
    """Discover files with sorting - convenience wrapper around list_files_asynch."""
    print(f"Discovering files in: {path}")
    files = self.list_files_asynch(path, parallel_discovery_threshold)
    files.sort(key=lambda x: x[1], reverse=(sort_order == 'desc'))
    return files
```

### 2. OrderTradeFeatureJoin Class
**File:** `/feature_engineering/order_trade_join.py`

New class implementing the join logic:
```python
class OrderTradeFeatureJoin:
    def __init__(self, bar_duration_ms: int, max_retries: int = 3)
    
    # Discovery methods
    def discover_l2q_files(data_access, features_path, sort_order) -> list[tuple[str, int]]
    def discover_trade_files(data_access, features_path, sort_order) -> list[tuple[str, int]]
    
    # File pairing logic
    def pair_files(l2q_files, trade_files) -> tuple[list[tuple], list[str], list[str]]
    
    # Custom grouping for pairs
    def group_file_pairs_for_processing(file_pairs) -> list[list[tuple]]
    
    # Join computation
    def join_features(l2q_path: str, trade_path: str) -> pl.LazyFrame
    
    # Pipeline integration methods
    def get_failed_items(results) -> list
    def discover_files(data_access, features_path, sort_order, discovery_mode) -> list[tuple]
```

### 3. Pipeline Integration
**Files:** `/pipeline/config.py`, `/pipeline/pipeline.py`

Configuration updates:
- Add `feature_join: OrderTradeFeatureJoin | None = None` to ProcessingConfig
- Add `l2q_trade_combined: StorageLocation` to StorageConfig
- Add feature join step with try-catch and inventory fallback
- Add join-specific retry logic and logging

### 4. Workflow Configuration
**File:** `/pipeline_workflow/order_flow_feature_join.py`

New workflow script for running the join step independently.

## Technical Details

### File Discovery Strategy
- **L2Q Files:** `s3://orderflowanalysis/intermediate/features/**/level2q/**/*.parquet`
- **Trade Files:** `s3://orderflowanalysis/intermediate/features/**/trades/**/*.parquet`
- **Discovery Mode:** Async with inventory fallback (same as feature engineering)

### File Pairing Logic
- **Exact Path Matching:** Replace `/level2q/` ↔ `/trades/` in file paths
- **Example:** 
  - L2Q: `features/2024/08/05/level2q/AMERICAS/XNAS-20240805_features_250ms.parquet`
  - Trade: `features/2024/08/05/trades/AMERICAS/XNAS-20240805_features_250ms.parquet`
- **Unmatched Files:** Track and report at end of job

### Grouping Strategy
- **Pair Creation:** `[(l2q_path, trade_path, max(l2q_size, trade_size))]`
- **Grouping Logic:** Group pairs by maximum file size per pair
- **Balancing:** Each Ray shard gets roughly equal maximum file sizes

### Join Processing
- **Join Keys:** `['bar_id', 'TradeDate', 'Ticker', 'ISOExchangeCode', 'MIC']`
- **Join Type:** Full outer join
- **Missing Data:** Forward fill for gaps
- **Implementation:**
```python
def join_features(self, l2q_path: str, trade_path: str) -> pl.LazyFrame:
    l2q_features = self.data_access.read(l2q_path)
    trade_features = self.data_access.read(trade_path)
    
    join_keys = ['bar_id', 'TradeDate', 'Ticker', 'ISOExchangeCode', 'MIC']
    result = l2q_features.join(trade_features, on=join_keys, how='full', suffix='_trade')
    return result.with_columns([pl.all().forward_fill()])
```

### Output Structure
- **Location:** `s3://orderflowanalysis/intermediate/features/l2q_trade_combined/`
- **Format:** Parquet files with combined L2Q + Trade features
- **Naming:** `TICKER-EXCHANGE-YYYYMMDD_combined_features_250ms.parquet`

## Logging and Monitoring

### Processing Logs
- Memory usage and row counts (similar to feature engineering)
- File pairing statistics
- Join success/failure rates
- Processing time per group

### End-of-Job Reporting
- Total files processed
- Successful joins vs failures
- **Unmatched Files Report:**
  - L2Q files without corresponding Trade files
  - Trade files without corresponding L2Q files
  - Reasons for mismatches

### Inventory Management
- **Inventory File:** `feature_join_input_inventory.csv`
- **Fallback:** Read from inventory when discovery fails due to credentials
- **Cache:** Write successful discoveries to inventory

## Error Handling

### Retry Logic
- Same retry mechanism as feature engineering
- Failed pairs re-grouped and retried
- Maximum retry attempts configurable

### Credential Issues
- Try-catch around discovery with NoCredentialsError
- Fallback to inventory file when discovery fails
- Same pattern as feature engineering step

### Missing Files
- Handle cases where only L2Q or only Trade features exist
- Full outer join ensures no data loss
- Forward fill handles missing values

## Configuration Example

```python
config = PipelineConfig(
    processing=ProcessingConfig(
        feature_join=OrderTradeFeatureJoin(
            bar_duration_ms=250,
            max_retries=3
        )
    ),
    storage=StorageConfig(
        features=S3Location(path='s3://orderflowanalysis/intermediate/features'),
        l2q_trade_combined=S3Location(path='s3://orderflowanalysis/intermediate/features/l2q_trade_combined'),
        metadata=S3Location(path='s3://orderflowanalysis/metadata')
    )
)
```

## Implementation Checklist

### Phase 1: Core Implementation
- [ ] Add `discover_files_asynch()` to data access layer
- [ ] Create `OrderTradeFeatureJoin` class
- [ ] Implement file pairing logic
- [ ] Implement custom grouping for pairs
- [ ] Add join computation with forward fill

### Phase 2: Pipeline Integration
- [ ] Update pipeline configuration classes
- [ ] Add feature join step to pipeline
- [ ] Add try-catch with inventory fallback
- [ ] Add retry logic and logging

### Phase 3: Workflow and Testing
- [ ] Create workflow script
- [ ] Add comprehensive logging
- [ ] Implement unmatched files reporting
- [ ] Test with sample data

### Phase 4: Documentation and Deployment
- [ ] Update configuration examples
- [ ] Add usage documentation
- [ ] Deploy and validate in production

## Files Modified/Created

### Modified Files (5)
1. `/data_preprocessing/data_access/s3.py` - Add discovery convenience method
2. `/pipeline/config.py` - Add feature join configuration
3. `/pipeline/pipeline.py` - Add join step and inventory logic
4. `/pipeline_workflow/order_flow_feature_engineering.py` - Add l2q_trade_combined storage

### New Files (2)
1. `/feature_engineering/order_trade_join.py` - Main join implementation
2. `/pipeline_workflow/order_flow_feature_join.py` - Workflow script

**Total Implementation:** ~50 lines of new code + configuration changes

## Success Metrics

- **Data Completeness:** All available L2Q and Trade features successfully joined
- **Performance:** Processing time comparable to individual feature engineering steps
- **Reliability:** <1% failure rate with proper retry handling
- **Monitoring:** Clear visibility into unmatched files and processing statistics