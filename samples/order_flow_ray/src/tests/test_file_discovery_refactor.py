#!/usr/bin/env python3

# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: MIT-0

"""Test refactored file discovery functionality."""
import sys
import os

# Setup path
current_dir = os.path.dirname(os.path.abspath(__file__))
src_dir = os.path.dirname(current_dir)
sys.path.append(src_dir)

from data_preprocessing.data_access.factory import DataAccessFactory
from feature_engineering.order_flow import OrderFlowFeatureEngineering

def test_file_discovery():
    """Test the refactored file discovery."""
    
    # Configuration
    REGION = 'us-east-1'
    PROFILE = 'blitvinfdp'
    
    print("Testing refactored file discovery...")
    
    # Test 1: Direct data access method
    print("\n1. Testing data_access.discover_files_asynch()...")
    data_access = DataAccessFactory.create('s3', region=REGION, profile_name=PROFILE)
    
    try:
        files = data_access.discover_files_asynch(
            's3://orderflowanalysis/intermediate/features', 
            sort_order='desc'
        )
        print(f"✅ Direct method: Found {len(files)} files")
        if files:
            print(f"   Largest file: {files[0][0]} ({files[0][1]:.2f} GB)")
            print(f"   Smallest file: {files[-1][0]} ({files[-1][1]:.2f} GB)")
    except Exception as e:
        print(f"❌ Direct method failed: {e}")
    
    # Test 2: Feature engineering wrapper
    print("\n2. Testing OrderFlowFeatureEngineering.discover_files_asynch()...")
    fe = OrderFlowFeatureEngineering(bar_duration_ms=250)
    
    try:
        files = fe.discover_files_asynch(
            data_access, 
            's3://orderflowanalysis/intermediate/features',
            'desc'
        )
        print(f"✅ Wrapper method: Found {len(files)} files")
        if files:
            print(f"   Largest file: {files[0][0]} ({files[0][1]:.2f} GB)")
            print(f"   Smallest file: {files[-1][0]} ({files[-1][1]:.2f} GB)")
    except Exception as e:
        print(f"❌ Wrapper method failed: {e}")
    
    # Test 3: Original discover_files method
    print("\n3. Testing OrderFlowFeatureEngineering.discover_files()...")
    try:
        files = fe.discover_files(
            data_access,
            's3://orderflowanalysis/intermediate/features',
            'desc',
            'asynch'
        )
        print(f"✅ Original method: Found {len(files)} files")
        if files:
            print(f"   Largest file: {files[0][0]} ({files[0][1]:.2f} GB)")
            print(f"   Smallest file: {files[-1][0]} ({files[-1][1]:.2f} GB)")
    except Exception as e:
        print(f"❌ Original method failed: {e}")
    
    print("\n✅ File discovery refactoring test completed!")

if __name__ == '__main__':
    test_file_discovery()