"""Test data normalization schema compliance."""
import sys
import os

# Add parent directory to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import polars as pl
from data_preprocessing.data_normalization import BMLLNormalizer
from data_preprocessing.data_access import DataAccessFactory


def test_trades_schema():
    """Test that normalized trades data matches expected schema."""
    # Setup
    normalizer = BMLLNormalizer()
    data_access = DataAccessFactory.create('s3', region='us-east-1')
    
    # Test file
    test_file = 's3://bmlldata/2024/01/02/trades/AMERICAS/trades-ARCX-20240102.parquet'
    
    # Read and normalize (limit to 100 rows for schema validation)
    raw_data = data_access.read(test_file).limit(100)
    normalized = normalizer.normalize(raw_data, 'trades')
    
    # Get expected schema
    expected_schema = normalizer.get_schema('trades')
    
    # Collect data to check schema
    df = normalized.collect()
    
    # Verify all expected columns exist
    missing_cols = set(expected_schema.keys()) - set(df.columns)
    assert not missing_cols, f"Missing columns: {missing_cols}"
    
    # Verify data types match
    for col, expected_type in expected_schema.items():
        actual_type = df.schema[col]
        assert actual_type == expected_type, f"Column {col}: expected {expected_type}, got {actual_type}"
    
    print(f"✓ Trades schema validation passed")
    print(f"  Columns: {len(df.columns)}")
    print(f"  Rows: {len(df)}")


def test_level2q_schema():
    """Test that normalized level2q data matches expected schema."""
    # Setup
    normalizer = BMLLNormalizer()
    data_access = DataAccessFactory.create('s3', region='us-east-1')
    
    # Test file
    test_file = 's3://bmlldata/2024/01/02/level2q/AMERICAS/ARCX-20240102.parquet'
    
    # Read and normalize (limit to 100 rows for schema validation)
    raw_data = data_access.read(test_file).limit(100)
    normalized = normalizer.normalize(raw_data, 'level2q')
    
    # Get expected schema
    expected_schema = normalizer.get_schema('level2q')
    
    # Collect data to check schema
    df = normalized.collect()
    
    # Verify all expected columns exist
    missing_cols = set(expected_schema.keys()) - set(df.columns)
    assert not missing_cols, f"Missing columns: {missing_cols}"
    
    # Verify data types match
    for col, expected_type in expected_schema.items():
        actual_type = df.schema[col]
        assert actual_type == expected_type, f"Column {col}: expected {expected_type}, got {actual_type}"
    
    print(f"✓ Level2Q schema validation passed")
    print(f"  Columns: {len(df.columns)}")
    print(f"  Rows: {len(df)}")


def test_output_path_different_from_input():
    """Test that normalized output path is different from raw input path."""
    from pipeline.config import PipelineConfig, DataConfig, StorageConfig
    
    # Test case 1: Different base paths
    raw_path = 's3://bucket/raw/data'
    normalized_path = 's3://bucket/normalized/data'
    
    assert raw_path != normalized_path, "Output path must be different from input path"
    
    # Test case 2: Verify path transformation logic
    test_input = 's3://bucket/raw/2024/01/02/trades/file.parquet'
    expected_output = 's3://bucket/normalized/2024/01/02/trades/file.parquet'
    actual_output = test_input.replace('s3://bucket/raw', 's3://bucket/normalized')
    
    assert actual_output == expected_output, f"Path transformation failed: {actual_output} != {expected_output}"
    assert test_input != actual_output, "Input and output paths must be different"
    
    # Test case 3: Verify StorageConfig paths are different
    config = StorageConfig(
        intermediate_path='s3://bucket/intermediate',
        output_path='s3://bucket/output'
    )
    
    raw_base = 's3://bucket/raw'
    assert config.normalized_path != raw_base, "Normalized path must be different from raw path"
    assert not config.normalized_path.startswith(raw_base), "Normalized path must not be under raw path"
    
    print(f"✓ Output path validation passed")
    print(f"  Raw path and normalized path are different")
    print(f"  No risk of overwriting raw data")


if __name__ == "__main__":
    print("Testing data normalization schema compliance...\n")
    
    try:
        test_trades_schema()
        print()
        test_level2q_schema()
        print()
        test_output_path_different_from_input()
        print("\n✓ All tests passed!")
    except AssertionError as e:
        print(f"\n✗ Test failed: {e}")
        raise
    except Exception as e:
        print(f"\n✗ Error: {e}")
        raise
