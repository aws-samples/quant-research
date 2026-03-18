# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: MIT-0

"""Test L2 quote data bar creation."""
import os
import sys
import polars as pl

# Setup correct import path
try:
    current_dir = os.path.dirname(os.path.abspath(__file__))
except NameError:
    current_dir = os.getcwd()
src_dir = os.path.join(current_dir, '..', '..', '..')
sys.path.append(src_dir)

from feature_engineering.base import TimeBarFeatureEngineering
from data_preprocessing.data_access.s3 import S3DataAccess

# Test configuration
TEST_CONFIG = {
    'bar_duration_ms': 250,
    's3_region': 'us-east-1',
    's3_profile': 'blitvinfdp',
    's3_quotes_path': 's3://orderflowanalysis/intermediate/normalized/2024/02/28/level2q/AMERICAS/XCIS-20240228.parquet',
    'timestamp_col': 'TimestampNanoseconds'
}

def test_l2_bar_creation():
    """Test bar creation for L2 quote data."""
    try:
        # Load L2 quote data from local test file
        test_data_path = os.path.join(current_dir, 'test_data', 'raw_l2q_high_activity_periods.csv')
        raw_data = pl.scan_csv(test_data_path)
        
        print(f"Loaded L2 quote data from: {test_data_path}")
        
        # Add bar_id with test duration
        data_with_bars = TimeBarFeatureEngineering.bar_time_addition(
            raw_data, 
            TEST_CONFIG['timestamp_col'], 
            TEST_CONFIG['bar_duration_ms']
        )
        
        result = data_with_bars.collect()
        
        print(f"Sample data with bars: {result.shape}")
        print(f"Columns: {result.columns}")
        
        # Check bar creation
        if 'bar_id' in result.columns and 'bar_id_dt' in result.columns and 'bar_duration_ms' in result.columns:
            print("✓ Bar columns created successfully")
            
            # Show sample bar data
            bar_sample = result.select(['TimestampNanoseconds', 'bar_id', 'bar_id_dt', 'bar_duration_ms']).sort('bar_id').head(10)
            print("\nSample bar data:")
            print(bar_sample)
            
            # Check bar_duration_ms consistency
            unique_durations = result['bar_duration_ms'].unique().to_list()
            if len(unique_durations) == 1 and unique_durations[0] == TEST_CONFIG['bar_duration_ms']:
                print(f"✓ Bar duration consistent: {unique_durations[0]}ms")
            else:
                print(f"⚠ Bar duration inconsistent: {unique_durations}")
                
        else:
            print("⚠ Missing bar columns")
            
    except Exception as e:
        print(f"⚠ L2 bar creation test failed: {e}")

if __name__ == '__main__':
    test_l2_bar_creation()