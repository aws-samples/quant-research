#!/usr/bin/env python3

# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: MIT-0

"""Test sequential prefix discovery."""
import sys
import os

# Setup path
current_dir = os.path.dirname(os.path.abspath(__file__))
sys.path.append(current_dir)

from data_preprocessing.data_access.factory import DataAccessFactory

def test_sequential_discovery():
    """Test sequential prefix discovery."""
    
    # Configuration
    REGION = 'us-east-1'
    PROFILE = 'blitvinfdp'
    BASE_PATH = 's3://orderflowanalysis/intermediate/repartitioned_v2'
    THRESHOLD = 1000
    
    # Initialize data access
    data_access = DataAccessFactory.create('s3', region=REGION, profile_name=PROFILE)
    
    # Parse path
    bucket = BASE_PATH.replace('s3://', '').split('/')[0]
    base_prefix = '/'.join(BASE_PATH.replace('s3://', '').split('/')[1:]) + '/'
    
    print(f"Testing sequential discovery on: {BASE_PATH}")
    print(f"Bucket: {bucket}")
    print(f"Base prefix: {base_prefix}")
    print(f"Threshold: {THRESHOLD}")
    
    # Test the sequential discovery function
    prefixes = data_access._discover_prefixes_sequential(bucket, base_prefix, THRESHOLD)
    
    print(f"\n✅ Sequential discovery completed")
    print(f"Found {len(prefixes)} prefixes")
    print(f"Sample prefixes:")
    for i, prefix in enumerate(prefixes[:10]):
        print(f"  {i+1}. {prefix}")
    
    return prefixes

if __name__ == '__main__':
    test_sequential_discovery()