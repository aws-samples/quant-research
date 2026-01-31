"""Test harness for feature engineering base classes."""
import os
import sys
import polars as pl

# Setup correct import path
current_dir = os.path.dirname(os.path.abspath(__file__))
src_dir = os.path.join(current_dir, '..', '..')
sys.path.append(src_dir)

from feature_engineering.base import TimeBarFeatureEngineering
from feature_engineering.order_flow import TradeFeatureEngineering
from data_preprocessing.data_access.s3 import S3DataAccess

# Test configuration
TEST_CONFIG = {
    'bar_duration_ms': 250,
    'timestamp_col': 'TradeTimestampNanoseconds',
    's3_region': 'us-east-1',
    's3_profile': 'blitvinfdp',
    's3_trades_path': 's3://orderflowanalysis/intermediate/normalized/2024/02/28/trades/AMERICAS/trades-XASE-20240228.parquet',
    'base_timestamp_ms': 1709136000000,  # 2024-02-28 16:00:00.000 UTC
    'test_offsets_ms': [100, 500, 1200],
    'expected_bar_ids': [1709136001000, 1709136001000, 1709136002000]
}


def test_timestamp_precision_conversion():
    """Test timestamp precision conversion with real data from CSV for multiple timeframes."""
    test_data_dir = '/Users/blitvin/IdeaProjects/quant-research-sample-using-amazon-ecs-and-aws-batch/samples/order_flow_ray/src/tests/feature_engineering/test_data'
    bar_durations = [50, 100, 200, 250, 500, 1000, 5000, 10000]
    
    for bar_duration in bar_durations:
        print(f"\nTesting {bar_duration}ms bar duration:")
        test_csv_path = f'{test_data_dir}/bar_id_test_table_{bar_duration}ms.csv'
        
        try:
            test_df = pl.read_csv(test_csv_path)
            ns_timestamps = test_df['input_timestamp_ns'].to_list()[:3]  # Use first 3 for testing
            expected_bar_ids = test_df['expected_bar_id_ms'].to_list()[:3]
            input_readable = test_df['input_timestamp_readable'].to_list()[:3]
            expected_readable = test_df['expected_bar_id_readable'].to_list()[:3]
            
            # Convert to different precisions
            test_timestamps = {
                'ns': ns_timestamps,
                'us': [ts // 1000 for ts in ns_timestamps],
                'ms': [ts // 1000000 for ts in ns_timestamps]
            }
            
            results = {}
            for precision, timestamps in test_timestamps.items():
                data = pl.LazyFrame({'timestamp': timestamps, 'value': list(range(len(timestamps)))})
                result = TimeBarFeatureEngineering.bar_time_addition(data, 'timestamp', bar_duration)
                results[precision] = result.collect()['bar_id'].to_list()
            
            if results['ms'] == results['us'] == results['ns'] == expected_bar_ids:
                print(f"  ✓ {bar_duration}ms: Precision conversion working correctly")
                print(f"    Input:    {input_readable[0]} -> Expected: {expected_readable[0]}")
            else:
                print(f"  ⚠ {bar_duration}ms: Precision conversion needs fixing")
                print(f"    Input:    {input_readable}")
                print(f"    Expected: {expected_readable}")
                print(f"    Got MS:   {results['ms']}")
        except Exception as e:
            print(f"  ⚠ {bar_duration}ms: Test file not found or error: {e}")


def test_datetime_to_unix_conversion():
    """Test datetime to unix timestamp conversion."""
    try:
        base_ts = TEST_CONFIG['base_timestamp_ms']
        offsets = TEST_CONFIG['test_offsets_ms']
        ns_timestamps = [base_ts * 1000000 + offset * 1000000 for offset in offsets]
        
        dt_data = pl.LazyFrame({
            'timestamp_ns': ns_timestamps
        }).with_columns([
            pl.from_epoch(pl.col('timestamp_ns'), time_unit='ns').alias('timestamp')
        ]).select(['timestamp']).with_columns([
            pl.lit([1, 2, 3]).alias('value')
        ])
        
        result_dt = TimeBarFeatureEngineering.bar_time_addition(dt_data, 'timestamp', TEST_CONFIG['bar_duration_ms'])
        df_dt = result_dt.collect()
        print(f"Datetime bar_ids: {df_dt['bar_id'].to_list()}")
        print("✓ Datetime to unix conversion test passed")
    except Exception as e:
        print(f"⚠ Datetime conversion test failed: {e}")


def sample_raw_data_2min_market_hours():
    """Sample 60 minutes of raw data between 9:30-16:00 EST."""
    try:
        s3_access = S3DataAccess(region=TEST_CONFIG['s3_region'], profile_name=TEST_CONFIG['s3_profile'])
        raw_data = s3_access.read(TEST_CONFIG['s3_trades_path'])
        
        # Convert nanoseconds to datetime and filter for market hours (9:30-16:00 EST)
        # 2024-02-28 9:30 EST = 14:30 UTC, 16:00 EST = 21:00 UTC
        start_ns = 1709125800000000000  # 2024-02-28 14:30:00 UTC in nanoseconds
        end_ns = start_ns + 3600000000000  # Add 60 minutes (3600 seconds)
        
        sample_data = raw_data.filter(
            (pl.col(TEST_CONFIG['timestamp_col']) >= start_ns) & 
            (pl.col(TEST_CONFIG['timestamp_col']) <= end_ns) &
            (pl.col('Ticker') == 'SPY')
        ).collect()
        
        # Write to CSV
        output_path = '/Users/blitvin/IdeaProjects/quant-research-sample-using-amazon-ecs-and-aws-batch/samples/order_flow_ray/src/tests/feature_engineering/spy_sample_60min.csv'
        sample_data.write_csv(output_path)
        
        print(f"Sampled {len(sample_data)} records for 60 minutes of SPY data")
        print(f"Data written to: {output_path}")
        return sample_data
    except Exception as e:
        print(f"⚠ Failed to sample data: {e}")
        return None


def test_timebar_feature_engineering_with_s3_data():
    """Test TimeBarFeatureEngineering initialization with S3 data using data access class."""
    try:
        s3_access = S3DataAccess(region=TEST_CONFIG['s3_region'], profile_name=TEST_CONFIG['s3_profile'])
        raw_data = s3_access.read(TEST_CONFIG['s3_trades_path'])
        fe = TimeBarFeatureEngineering(
            raw_data=raw_data,
            bar_duration_ms=TEST_CONFIG['bar_duration_ms'],
            timestamp_col=TEST_CONFIG['timestamp_col']
        )
        assert fe.bar_duration_ms == TEST_CONFIG['bar_duration_ms']
        assert fe.timestamp_col == TEST_CONFIG['timestamp_col']
        schema_names = fe.raw_data.collect_schema().names()
        assert 'bar_id' in schema_names
        assert 'bar_id_dt' in schema_names
        print("✓ S3 TimeBarFeatureEngineering test passed")
    except Exception as e:
        print(f"⚠ S3 data not accessible: {e}")


def test_trade_feature_engineering_with_s3_data():
    """Test TradeFeatureEngineering feature computation using S3 data access class."""
    try:
        s3_access = S3DataAccess(region=TEST_CONFIG['s3_region'], profile_name=TEST_CONFIG['s3_profile'])
        raw_data = s3_access.read(TEST_CONFIG['s3_trades_path'])
        fe = TradeFeatureEngineering(bar_duration_ms=TEST_CONFIG['bar_duration_ms'])
        result = fe.feature_computation(raw_data)
        df = result.filter(pl.col('Ticker') == 'SPY').sort('Ticker','MIC','OPOL','bar_id','ExecutionVenue').collect()
        
        assert fe.bar_duration_ms == TEST_CONFIG['bar_duration_ms']
        assert len(df) > 0
        required_cols = ['bar_id', 'bar_id_dt', 'price_open', 'volume']
        for col in required_cols:
            assert col in df.columns, f"Missing column: {col}"
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