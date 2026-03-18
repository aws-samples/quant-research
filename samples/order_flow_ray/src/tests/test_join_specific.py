#!/usr/bin/env python3

# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: MIT-0

"""Test join with specific file pair."""
import sys
import os

# Setup path
try:
    current_dir = os.path.dirname(os.path.abspath(__file__))
except NameError:
    # Running in console/notebook
    current_dir = '/Users/blitvin/IdeaProjects/quant-research-sample-using-amazon-ecs-and-aws-batch/samples/order_flow_ray/src/tests'
src_dir = os.path.dirname(current_dir)
sys.path.append(src_dir)

import polars as pl
from feature_engineering.order_trade_join import OrderTradeFeatureJoin
from data_preprocessing.data_access.factory import DataAccessFactory

def test_join():
    # File paths
    l2q_path = "s3://orderflowanalysis/intermediate/features/2024/11/29/level2q/AMERICAS/Y/XCHI-20241129_features_250ms.parquet"
    trade_path = "s3://orderflowanalysis/intermediate/features/2024/11/29/trades/AMERICAS/Y/trades-XCHI-20241129_features_250ms.parquet"
    
    # Initialize
    data_access = DataAccessFactory.create('s3', region='us-east-1', profile_name='blitvinfdp')
    join_eng = OrderTradeFeatureJoin(bar_duration_ms=250)


    trade_df = data_access.read(trade_path)
    l2q_df = data_access.read(l2q_path)

    # Get storage options
    storage_options = data_access.get_storage_options()
    
    # Perform join
    joined_features = join_eng.join_features(l2q_path, trade_path, storage_options)
    joined_features_mt = joined_features.collect()
    
    # Get row count
    if isinstance(joined_features, pl.LazyFrame):
        row_count = joined_features.select(pl.len()).collect().item()
    else:
        row_count = joined_features.select(pl.len()).item()
    
    print(f"Joined features row count: {row_count}")
    print(f"Expected: 27,777 rows")

if __name__ == "__main__":
    test_join()