#!/usr/bin/env python3

# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: MIT-0

"""Test pairing logic for OrderTradeFeatureJoin using repartitioned data."""

import sys
import os

# Setup path
#current_dir = os.path.dirname(os.path.abspath(__file__))
sys.path.append(current_dir)

from feature_engineering.order_trade_join import OrderTradeFeatureJoin
from data_preprocessing.data_access.factory import DataAccessFactory


def test_pairing_logic():
    """Test file pairing logic with real repartitioned data."""
    print("Testing OrderTradeFeatureJoin pairing logic...")
    
    # Create instances
    data_access = DataAccessFactory.create('s3', region='us-east-1', profile_name='blitvinfdp')
    join_processor = OrderTradeFeatureJoin(bar_duration_ms=250)
    
    # Use repartitioned data path (sequential discovery)
    test_path = 's3://orderflowanalysis/intermediate/repartitioned_v3/2024/04/04/'
    print(f"Discovering files in: {test_path}")
    
    # Discover files sequentially (not async to avoid Ray overhead)
    all_files = data_access.list_files(test_path)
    print(f"Found {len(all_files)} total files")
    
    # Split into L2Q and Trade files
    l2q_files = [(path, size) for path, size in all_files if '/level2q/' in path]
    trade_files = [(path, size) for path, size in all_files if '/trades/' in path]
    
    print(f"L2Q files: {len(l2q_files)}")
    print(f"Trade files: {len(trade_files)}")
    
    if l2q_files:
        print(f"Sample L2Q files:")
        for i, (path, size) in enumerate(l2q_files[:3]):
            print(f"  {i+1}. {path}")
    
    if trade_files:
        print(f"Sample Trade files:")
        for i, (path, size) in enumerate(trade_files[:3]):
            print(f"  {i+1}. {path}")
    
    # Test pairing logic
    print(f"\nTesting pairing logic...")
    paired_files, unmatched_l2q, unmatched_trade = join_processor.pair_files(l2q_files, trade_files)
    
    print(f"Results:")
    print(f"  Paired: {len(paired_files)}")
    print(f"  Unmatched L2Q: {len(unmatched_l2q)}")
    print(f"  Unmatched Trade: {len(unmatched_trade)}")
    
    if paired_files:
        print(f"\nSample paired files:")
        for i, (l2q_path, trade_path, max_size) in enumerate(paired_files[:3]):
            print(f"  {i+1}. L2Q: {l2q_path.split('/')[-1]}")
            print(f"     Trade: {trade_path.split('/')[-1]}")
            print(f"     Max size: {max_size:.3f} GB")
    
    if unmatched_l2q:
        print(f"\nSample unmatched L2Q files:")
        for i, path in enumerate(unmatched_l2q[:3]):
            print(f"  {i+1}. {path.split('/')[-1]}")
    
    if unmatched_trade:
        print(f"\nSample unmatched Trade files:")
        for i, path in enumerate(unmatched_trade[:3]):
            print(f"  {i+1}. {path.split('/')[-1]}")


if __name__ == "__main__":
    test_pairing_logic()