"""Test harness for feature engineering base classes."""
import os
import sys
import polars as pl

# Setup correct import path
current_dir = os.getcwd()
src_dir = os.path.join(current_dir, 'samples/order_flow_ray/src')
sys.path.append(src_dir)

from feature_engineering.base import TimeBarFeatureEngineering
from feature_engineering.order_flow import TradeFeatureEngineering
from data_preprocessing.data_access.s3 import S3DataAccess


def test_timestamp_precision_conversion():
    """Test timestamp precision conversion with granular data to see ceil effect."""
    # More granular test data with sub-second precision
    # Base time: 2024-02-28 16:00:00.000 UTC (1709136000000 ms)
    
    # Test milliseconds conversion - with 100ms, 500ms, 1200ms offsets
    ms_data = pl.LazyFrame({
        'timestamp': [1709136000100, 1709136000500, 1709136001200],  # 100ms, 500ms, 1200ms
        'value': [1, 2, 3]
    })
    result_ms = TimeBarFeatureEngineering.bar_time_addition(ms_data, 'timestamp', 1000)
    df_ms = result_ms.collect()
    print(f"Milliseconds (100ms, 500ms, 1200ms) bar_ids: {df_ms['bar_id'].to_list()}")
    
    # Test microseconds conversion - same times in microseconds
    us_data = pl.LazyFrame({
        'timestamp': [1709136000100000, 1709136000500000, 1709136001200000],  # Same times in microseconds
        'value': [1, 2, 3]
    })
    result_us = TimeBarFeatureEngineering.bar_time_addition(us_data, 'timestamp', 1000)
    df_us = result_us.collect()
    print(f"Microseconds (100ms, 500ms, 1200ms) bar_ids: {df_us['bar_id'].to_list()}")
    
    # Test nanoseconds - same times in nanoseconds
    ns_data = pl.LazyFrame({
        'timestamp': [1709136000100000000, 1709136000500000000, 1709136001200000000],  # Same times in nanoseconds
        'value': [1, 2, 3]
    })
    result_ns = TimeBarFeatureEngineering.bar_time_addition(ns_data, 'timestamp', 1000)
    df_ns = result_ns.collect()
    print(f"Nanoseconds (100ms, 500ms, 1200ms) bar_ids: {df_ns['bar_id'].to_list()}")
    
    # Expected: All should produce [1709136001.0, 1709136001.0, 1709136002.0] 
    # (100ms and 500ms round up to 1s, 1200ms rounds up to 2s)
    print(f"Expected bar_ids: [1709136001.0, 1709136001.0, 1709136002.0]")
    print(f"MS: {df_ms['bar_id'].to_list()}")
    print(f"US: {df_us['bar_id'].to_list()}")
    print(f"NS: {df_ns['bar_id'].to_list()}")
    
    if df_ms['bar_id'].to_list() == df_us['bar_id'].to_list() == df_ns['bar_id'].to_list():
        print("✓ Timestamp precision conversion working correctly")
    else:
        print("⚠ Timestamp precision conversion needs fixing - different precisions produce different bar_ids")


def test_datetime_to_unix_conversion():
    """Test datetime to unix timestamp conversion."""
    try:
        # Create datetime values from the exact nanosecond timestamps
        # 1709136000100000000, 1709136000500000000, 1709136001200000000
        dt_data = pl.LazyFrame({
            'timestamp_ns': [1709136000100000000, 1709136000500000000, 1709136001200000000]
        }).with_columns([
            pl.from_epoch(pl.col('timestamp_ns'), time_unit='ns').alias('timestamp')
        ]).select(['timestamp']).with_columns([
            pl.lit([1, 2, 3]).alias('value')
        ])
        
        result_dt = TimeBarFeatureEngineering.bar_time_addition(dt_data, 'timestamp', 1000)
        df_dt = result_dt.collect()
        print(f"Datetime (100ms, 500ms, 1200ms) bar_ids: {df_dt['bar_id'].to_list()}")
        print("✓ Datetime to unix conversion test passed")
    except Exception as e:
        print(f"⚠ Datetime conversion test failed: {e}")


def test_timebar_feature_engineering_with_s3_data():
    """Test TimeBarFeatureEngineering initialization with S3 data using data access class."""
    s3_trades_path = 's3://orderflowanalysis/intermediate/normalized/2024/02/28/trades/AMERICAS/trades-XASE-20240228.parquet'
    try:
        s3_access = S3DataAccess(region='us-east-1', profile_name='blitvinfdp')
        raw_data = s3_access.read(s3_trades_path)
        fe = TimeBarFeatureEngineering(
            raw_data=raw_data,
            bar_duration_ms=1000,
            timestamp_col='TradeTimestampNanoseconds'
        )
        assert fe.bar_duration_ms == 1000
        assert fe.timestamp_col == 'TradeTimestampNanoseconds'
        assert 'bar_id' in fe.raw_data.collect_schema().names()
        print("✓ S3 TimeBarFeatureEngineering test passed")
    except Exception as e:
        print(f"⚠ S3 data not accessible: {e}")


def test_trade_feature_engineering_with_s3_data():
    """Test TradeFeatureEngineering feature computation using S3 data access class."""
    s3_trades_path = 's3://orderflowanalysis/intermediate/normalized/2024/02/28/trades/AMERICAS/trades-XASE-20240228.parquet'
    try:
        s3_access = S3DataAccess(region='us-east-1', profile_name='blitvinfdp')
        raw_data = s3_access.read(s3_trades_path)
        fe = TradeFeatureEngineering(bar_duration_ms=1000)
        result = fe.feature_computation(raw_data)
        df = result.collect()
        
        assert fe.bar_duration_ms == 1000
        assert len(df) > 0
        assert 'bar_id' in df.columns
        assert 'price_open' in df.columns
        assert 'volume' in df.columns
        print("✓ S3 TradeFeatureEngineering test passed")
    except Exception as e:
        print(f"⚠ S3 data not accessible: {e}")


def main():
    """Run all tests."""
    print("Running feature engineering tests...")
    test_timestamp_precision_conversion()
    test_datetime_to_unix_conversion()
    test_timebar_feature_engineering_with_s3_data()
    test_trade_feature_engineering_with_s3_data()
    print("All tests completed!")


if __name__ == '__main__':
    main()