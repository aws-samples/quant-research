# Change Log: S3 Tables → S3 Storage

## Rationale
S3 Tables (Iceberg) has unsolvable concurrency issues when multiple Ray workers write simultaneously. Switching back to standard S3 with Hive-style partitioning.

## Files Modified

### 1. Configuration
- **bmll_orchestrator.py**: Switched storage locations
  - `features`: S3TablesLocation → S3Location (`s3://orderflowanalysis/intermediate/features`)
  - `predictions`: S3TablesLocation → S3Location (`s3://orderflowanalysis/output/predictions`)
  - Removed S3TablesLocation import

### 2. Documentation
- **dev_rules.md**: Updated storage strategy
  - Removed "S3 Tables and Iceberg" section with complex partitioning rules
  - Added "S3 Storage" section with Hive-style partitioning recommendation
  - Simplified to standard S3 path structure: `YYYY/MM/DD/{data_type}/AMERICAS/{exchange}/`

## Files NOT Modified (Already Support Both)
- **pipeline/config.py**: Already has both S3Location and S3TablesLocation classes
- **pipeline/pipeline.py**: Already handles both storage types dynamically
- **data_preprocessing/data_access/**: Factory pattern supports both s3 and s3tables
- **tests/test_pipeline_normalization.py**: Already uses S3Location

## Test Files
- **tests/test_s3tables.py**: Kept for S3 Tables testing (deprecated, not used in main pipeline)
- **tests/test_s3_write.py**: NEW - Simple S3 write test for standard S3 operations

## Storage Configuration Summary

### Before (S3 Tables)
```python
features=S3TablesLocation(
    table_name='features',
    table_bucket_arn='arn:aws:s3tables:us-east-1:614393260192:bucket/order-flow-analysis-s3table',
    namespace='trading'
)
```

### After (S3)
```python
features=S3Location(path='s3://orderflowanalysis/intermediate/features')
```

## Benefits
- No Iceberg commit conflicts
- Simpler architecture
- Standard S3 operations
- Better Ray worker parallelism
- Hive-style partitioning still provides efficient filtering
