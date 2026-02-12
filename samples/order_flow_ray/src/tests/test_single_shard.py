#!/usr/bin/env python3
"""Test script for single shard feature engineering."""
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
from feature_engineering.order_flow import OrderFlowFeatureEngineering

def test_single_shard():
    """Test feature engineering on a single file."""
    
    # Configuration
    BAR_DURATION_MS = 250
    REGION = 'us-east-1'
    PROFILE = 'blitvinfdp'  # Use default profile
    
    # Initialize feature engineering
    fe = OrderFlowFeatureEngineering(bar_duration_ms=BAR_DURATION_MS)
    
    # Discover files
    print("Discovering files...")
    data_access = DataAccessFactory.create('s3', region=REGION, profile_name=PROFILE)
    discovered_files = fe.discover_files(data_access, 's3://orderflowanalysis/intermediate/repartitioned', 'desc')
    sort_order='desc'
    discovered_files.sort(key=lambda x: x[1], reverse=(sort_order == 'desc'))
    print(f"Found {len(discovered_files)} files")

    specific_files = ['s3://orderflowanalysis/intermediate/features/2024/12/24/l2q_trade_joined/AMERICAS/E/XCHI-20241224_joined_250ms.parquet',
                      's3://orderflowanalysis/intermediate/normalized/2023/03/13/level2q/AMERICAS/XNAS-20230313.parquet']
    specific_set = set(specific_files)
    filtered_files = [(path, size) for path, size in discovered_files if path in specific_set];filtered_files

    # Group files for processing
    print("\nGrouping files for processing...")
    grouped_files = fe.group_files_for_processing(discovered_files)
    print(f"Created {len(grouped_files)} groups")
    
    # Process first group
    if not grouped_files:
        print("No file groups found")

        
    first_group = grouped_files[0]
    print(f"\nProcessing first group with {len(first_group)} files:")
    for file_path in first_group:
        print(f"  {file_path}")
    
    # Apply feature engineering to first file in group
    test_file = specific_files[0]#first_group[0][0]  # Extract file path from tuple
    print(f"\nProcessing file: {test_file}")
    
    try:
        # Read normalized data
        print("Reading normalized data...")
        df = data_access.read(test_file)
        #df = df.filter(pl.col('Ticker') == 'SPY')
        #df = df.sort(['TradeDate', 'Ticker', 'ISOExchangeCode', 'MIC', 'ExchangeTicker', 'TimestampNanoseconds'])

        print(f"Data columns: {df.columns}")
        print(f"Data estimated rows: {df.select(pl.len()).collect().item()}")
        
        # Determine data type from path
        data_type = 'level2q' if 'level2q' in test_file else 'trades'
        print(f"Data type: {data_type}")
        
        # Initialize feature engineering
        feature_eng = OrderFlowFeatureEngineering(bar_duration_ms=BAR_DURATION_MS, max_section=2)
        
        # Apply feature engineering
        print("Computing features...")
        features = feature_eng.feature_computation(df, data_type)
        
        # Show feature info
        print(f"Feature columns: {features.columns}")
        print(f"Feature rows: {features.select(pl.len()).collect().item()}")
        
        # Show sample
        print("\nSample features:")
        print(features.head().collect())
        
        # Optional: Write to output location
        output_path = test_file.replace('/normalized/', '/features/').replace('.parquet', f'_features_{BAR_DURATION_MS}ms.parquet')
        print(f"\nWriting features to: {output_path}")
        data_access.write(features, output_path)
        
        print("✅ Feature engineering test completed successfully!")
        
    except Exception as e:
        print(f"❌ Error: {e}")
        import traceback
        traceback.print_exc()

if __name__ == '__main__':
    test_single_shard()