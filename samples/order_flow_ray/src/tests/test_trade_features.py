#!/usr/bin/env python3

# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: MIT-0

"""Test script for Trade feature engineering."""
import sys
import os
import polars as pl

# Setup path
try:
    current_dir = os.path.dirname(os.path.abspath(__file__))
except NameError:
    # Running in console/notebook
    current_dir = '/Users/blitvin/IdeaProjects/quant-research-sample-using-amazon-ecs-and-aws-batch/samples/order_flow_ray/src/tests'
src_dir = os.path.dirname(current_dir)
sys.path.append(src_dir)

from data_preprocessing.data_access.factory import DataAccessFactory
from feature_engineering.order_flow import TradeFeatureEngineering

def test_trade_features():
    """Test Trade feature engineering on specific file."""
    
    # Configuration
    BAR_DURATION_MS = 250
    REGION = 'us-east-1'
    PROFILE = 'blitvinfdp'
    TEST_FILE = 's3://orderflowanalysis/intermediate/repartitioned_v3/2024/12/24/trades/AMERICAS/L/trades-IEXG-20241224.parquet'
    
    print(f"Testing Trade feature engineering with file: {TEST_FILE}")
    print(f"Bar duration: {BAR_DURATION_MS}ms")
    
    try:
        # Initialize data access
        print("\n1. Initializing data access...")
        data_access = DataAccessFactory.create('s3', region=REGION, profile_name=PROFILE)
        
        # Read normalized data
        print("\n2. Reading normalized data...")
        df = data_access.read(TEST_FILE)
        print(f"   Lazy frame created")
        
        # Filter to SPY for testing
        print("\n3. Filtering to SPY ticker...")
        df = df.filter(pl.col('Ticker') == 'SPY')
        print(f"   Filtered to SPY only")
        
        # Initialize Trade feature engineering
        print("\n4. Initializing Trade feature engineering...")
        trade_eng = TradeFeatureEngineering(bar_duration_ms=BAR_DURATION_MS)
        
        # Test feature computation
        print("\n5. Computing trade features...")
        features = trade_eng.feature_computation(df)
        result = features.collect()
        print(f"   Trade features: {result.width} columns, {len(result)} rows")
        print(f"   Columns: {result.columns}")
        
        # Show sample data
        print("\n6. Sample results:")
        print(result.head(3))
        
        # Test output writing
        print("\n7. Testing output writing...")
        output_path = TEST_FILE.replace('/normalized/', '/features_test/').replace('.parquet', f'_trade_features_{BAR_DURATION_MS}ms.parquet')
        print(f"   Writing to: {output_path}")
        data_access.write(result, output_path)
        
        print("\n✅ Trade feature engineering test completed successfully!")
        print(f"   Final output: {result.width} features across {len(result)} bars")
        
    except Exception as e:
        print(f"\n❌ Error: {e}")
        import traceback
        traceback.print_exc()

if __name__ == '__main__':
    test_trade_features()