#!/usr/bin/env python3

# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: MIT-0

"""Test script for single date/type reconciliation."""
import sys
import os

# Setup path
try:
    current_dir = os.path.dirname(os.path.abspath(__file__))
except NameError:
    current_dir = '/Users/blitvin/IdeaProjects/quant-research-sample-using-amazon-ecs-and-aws-batch/samples/order_flow_ray/src/tests'
src_dir = os.path.dirname(current_dir)
sys.path.append(src_dir)

from data_preprocessing.data_access.factory import DataAccessFactory
from data_preprocessing.reconciliation import Reconciliation

def test_single_reconciliation():
    """Test reconciliation for a single date/type."""
    
    # Configuration
    REGION = 'us-east-1'
    PROFILE = 'blitvinfdp'
    date = '2023-01-23'
    data_type = 'trades'
    normalized_path = 's3://orderflowanalysis/intermediate/normalized'
    repartitioned_path = 's3://orderflowanalysis/intermediate/repartitioned_v2'
    
    print(f"Testing Reconciliation for {date} {data_type}...")
    
    # Create data access
    data_access = DataAccessFactory.create('s3', region=REGION, profile_name=PROFILE)
    
    # Create reconciliation instance
    recon = Reconciliation()
    
    # Call reconcile directly
    result = recon.reconcile(date, data_type, data_access, normalized_path, repartitioned_path)
    
    # Display results
    print("\nReconciliation Results:")
    if result.get('message') == 'success':
        matched = result.get('matched_groups', 0)
        mismatched = result.get('mismatched_groups', 0)
        total = result.get('total_groups', 0)
        
        print(f"  Total groups: {total:,}")
        print(f"  Matched groups: {matched:,}")
        print(f"  Mismatched groups: {mismatched:,}")
        
        if mismatched > 0:
            print(f"\n  ⚠️  Found {mismatched} mismatches")
            print("\nSample mismatches:")
            mismatches = result.get('mismatches')
            if mismatches is not None:
                print(mismatches.head(10))
        else:
            print(f"\n  ✅ All groups matched!")
    else:
        print(f"  ❌ Failed: {result.get('message', 'Unknown error')}")

if __name__ == '__main__':
    test_single_reconciliation()


'''Sample successful files:
s3://orderflowanalysis/intermediate/repartitioned_v2/2023/01/23/trades/AMERICAS/Z/CHI/XCHI/ARCX/XCHI/trades-XCHI-20230123.parquet -> s3://orderflowanalysis/intermediate/features/2023/01/23/trades/AMERICAS/Z/CHI/XCHI/ARCX/XCHI/trades-XCHI-20230123_features_250ms.parquet (1 rows)
s3://orderflowanalysis/intermediate/repartitioned_v2/2023/01/27/trades/AMERICAS/Z/CHI/XCHI/ARCX/XCHI/trades-XCHI-20230127.parquet -> s3://orderflowanalysis/intermediate/features/2023/01/27/trades/AMERICAS/Z/CHI/XCHI/ARCX/XCHI/trades-XCHI-20230127_features_250ms.parquet (1 rows)
s3://orderflowanalysis/intermediate/repartitioned_v2/2023/03/15/trades/AMERICAS/Z/CHI/XCHI/ARCX/XCHI/trades-XCHI-20230315.parquet -> s3://orderflowanalysis/intermediate/features/2023/03/15/trades/AMERICAS/Z/CHI/XCHI/ARCX/XCHI/trades-XCHI-20230315_features_250ms.parquet (1 rows)
s3://orderflowanalysis/intermediate/repartitioned_v2/2023/03/30/trades/AMERICAS/Z/CHI/XCHI/ARCX/XCHI/trades-XCHI-20230330.parquet -> s3://orderflowanalysis/intermediate/features/2023/03/30/trades/AMERICAS/Z/CHI/XCHI/ARCX/XCHI/trades-XCHI-20230330_features_250ms.parquet (1 rows)
s3://orderflowanalysis/intermediate/repartitioned_v2/2024/12/13/trades/AMERICAS/Z/CHI/XCHI/ARCX/XCHI/trades-XCHI-20241213.parquet -> s3://orderflowanalysis/intermediate/features/2024/12/13/trades/AMERICAS/Z/CHI/XCHI/ARCX/XCHI/trades-XCHI-20241213_features_250ms.parquet (1 rows)
'''