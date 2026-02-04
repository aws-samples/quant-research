#!/usr/bin/env python3
"""Test harness for feature engineering file discovery."""

import sys
import os
sys.path.append('/Users/blitvin/IdeaProjects/quant-research-sample-using-amazon-ecs-and-aws-batch/samples/order_flow_ray/src')

from data_preprocessing.data_access.factory import DataAccessFactory
from feature_engineering.order_flow import OrderFlowFeatureEngineering

def test_file_discovery():
    """Test feature engineering file discovery."""
    
    # Configuration
    region = 'us-east-1'
    profile_name = 'blitvinfdp'
    input_path = 's3://orderflowanalysis/intermediate/normalized'
    
    print("=== Feature Engineering File Discovery Test ===")
    print(f"Region: {region}")
    print(f"Profile: {profile_name}")
    print(f"Input path: {input_path}")
    print()
    
    try:
        # Create data access
        print("1. Creating data access...")
        data_access = DataAccessFactory.create('s3', region=region, profile_name=profile_name)
        print("✓ Data access created")
        
        # Create feature engineering instance
        print("\n2. Creating feature engineering instance...")
        feat_eng = OrderFlowFeatureEngineering(bar_duration_ms=1000)
        print("✓ Feature engineering instance created")
        
        # Test raw file listing first
        print(f"\n3. Testing raw file listing from {input_path}...")
        try:
            raw_files = data_access.list_files(input_path)
            print(f"✓ Found {len(raw_files)} total files")
            
            # Show first few files
            print("\nFirst 5 files:")
            for i, (file_path, size) in enumerate(raw_files[:5]):
                print(f"  {i+1}. {file_path} ({size:,} bytes)")
                
            # Filter for parquet files
            parquet_files = [(fp, size) for fp, size in raw_files if fp.endswith('.parquet')]
            print(f"\n✓ Found {len(parquet_files)} .parquet files")
            
        except Exception as e:
            print(f"✗ Raw file listing failed: {e}")
            return
        
        # Test discover_files method
        print(f"\n4. Testing discover_files method...")
        try:
            file_groups = feat_eng.discover_files(data_access, input_path, 'asc')
            print(f"✓ discover_files returned {len(file_groups)} groups")
            
            if file_groups:
                total_files = sum(len(group) for group in file_groups)
                print(f"✓ Total files across all groups: {total_files}")
                
                # Show first few groups
                print(f"\nFirst 3 groups:")
                for i, group in enumerate(file_groups[:3]):
                    print(f"  Group {i+1}: {len(group)} files")
                    for j, (file_path, size) in enumerate(group[:2]):  # Show first 2 files per group
                        print(f"    {j+1}. {file_path} ({size:,} bytes)")
                    if len(group) > 2:
                        print(f"    ... and {len(group)-2} more files")
            else:
                print("✗ No groups discovered - investigating...")
                
                # Debug path parsing
                print("\n5. Debugging path parsing...")
                for i, (file_path, size) in enumerate(parquet_files[:5]):
                    print(f"\nFile {i+1}: {file_path}")
                    parts = file_path.split('/')
                    print(f"  Parts: {parts}")
                    
                    if len(parts) >= 5:
                        yyyy, mm, dd, data_type, region = parts[-5:]
                        filename = parts[-1]
                        print(f"  Parsed: {yyyy}/{mm}/{dd}/{data_type}/{region}/{filename}")
                        
                        if data_type == 'trades':
                            exchange = filename.split('-')[1] if '-' in filename else 'unknown'
                            print(f"  Trades exchange: {exchange}")
                        elif data_type == 'level2q':
                            exchange = filename.split('-')[0] if '-' in filename else 'unknown'
                            print(f"  Level2q exchange: {exchange}")
                        else:
                            print(f"  Skipping data_type: {data_type}")
                    else:
                        print(f"  ✗ Not enough path parts (need 5, got {len(parts)})")
                        
        except Exception as e:
            print(f"✗ discover_files failed: {e}")
            import traceback
            traceback.print_exc()
            
    except Exception as e:
        print(f"✗ Test setup failed: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    test_file_discovery()