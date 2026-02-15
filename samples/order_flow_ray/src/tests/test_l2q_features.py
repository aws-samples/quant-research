#!/usr/bin/env python3
"""Test script for L2Q feature engineering stages."""
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
from feature_engineering.order_flow import L2QFeatureEngineering

def test_l2q_features():
    """Test L2Q feature engineering on specific file."""
    
    # Configuration
    BAR_DURATION_MS = 250
    REGION = 'us-east-1'
    PROFILE = 'blitvinfdp'
    TEST_FILE = 's3://orderflowanalysis/intermediate/normalized/2023/03/13/level2q/AMERICAS/XNAS-20230313.parquet'
    
    print(f"Testing L2Q feature engineering with file: {TEST_FILE}")
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
        
        # Initialize L2Q feature engineering
        print("\n4. Initializing L2Q feature engineering...")
        l2q_eng = L2QFeatureEngineering(bar_duration_ms=BAR_DURATION_MS)
        
        # Test each section individually
        print("\n5. Testing individual sections...")
        
        # Test Section 1 only
        print("\n   Testing Section 1 (Bar Metadata)...")
        l2q_eng_s1 = L2QFeatureEngineering(bar_duration_ms=BAR_DURATION_MS, max_section=1)
        features_s1 = l2q_eng_s1.feature_computation(df, max_section=1)
        result_s1 = features_s1.collect()
        print(f"   Section 1 features: {result_s1.width} columns, {len(result_s1)} rows")
        print(f"   Columns: {result_s1.columns}")
        
        # Test Section 2 only
        print("\n   Testing Section 2 (Quote Activity)...")
        l2q_eng_s2 = L2QFeatureEngineering(bar_duration_ms=BAR_DURATION_MS, max_section=2)
        features_s2 = l2q_eng_s2.feature_computation(df, max_section=2)
        result_s2 = features_s2.collect()
        print(f"   Section 1-2 features: {result_s2.width} columns, {len(result_s2)} rows")
        
        # Test all sections
        print("\n   Testing all sections (1-8)...")
        features_all = l2q_eng.feature_computation(df, max_section=None)
        result_all = features_all.collect()
        print(f"   All sections features: {result_all.width} columns, {len(result_all)} rows")
        
        # Show sample data
        print("\n6. Sample results:")
        print("   Section 1 sample:")
        print(result_s1.head(3))
        
        print("\n   All sections sample (first 10 columns):")
        sample_cols = result_all.columns[:10]
        print(result_all.select(sample_cols).head(3))
        
        # Test output writing
        print("\n7. Testing output writing...")
        output_path = TEST_FILE.replace('/normalized/', '/features_test/').replace('.parquet', f'_features_{BAR_DURATION_MS}ms.parquet')
        print(f"   Writing to: {output_path}")
        data_access.write(result_all.lazy(), output_path)
        
        print("\n✅ L2Q feature engineering test completed successfully!")
        print(f"   Final output: {result_all.width} features across {len(result_all)} bars")
        
    except Exception as e:
        print(f"\n❌ Error: {e}")
        import traceback
        traceback.print_exc()

if __name__ == '__main__':
    test_l2q_features()