#!/usr/bin/env python3

import sys
import os
sys.path.append('/Users/blitvin/IdeaProjects/quant-research-sample-using-amazon-ecs-and-aws-batch/samples/order_flow_ray/src')

import polars as pl
from feature_engineering import L2QFeatureEngineering

def main():
    # Load test data
    input_file = os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', '..', '..', 'tests', 'test_feature_engineering', 'l2_feed', 'test_data', 'raw_l2q_high_activity_periods.csv')
    output_file = os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', '..', '..', 'tests', 'test_feature_engineering', 'l2_feed', 'output_data', 'l2q_features_sections_1_2_3_4_5_6_7_8_250ms.csv')
    
    print(f"Loading data from: {input_file}")
    
    # Read CSV as LazyFrame
    data = pl.scan_csv(input_file)
    
    # Initialize L2Q feature engineering with 250ms bars
    fe = L2QFeatureEngineering(bar_duration_ms=250)
    
    print("Computing L2Q features...")
    
    # Compute features
    result = fe.feature_computation(data)
    
    # Collect and write to CSV
    result_df = result.collect()
    result_df.write_csv(output_file)
    
    print(f"Features computed and saved to: {output_file}")
    print(f"Output shape: {result_df.shape}")
    print(f"Columns: {result_df.columns}")

if __name__ == "__main__":
    main()