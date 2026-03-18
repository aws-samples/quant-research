# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: MIT-0

"""Extract raw L2Q data for selected high-activity periods and save as CSV."""
import os
import sys
import polars as pl

# Setup correct import path
current_dir = os.path.dirname(os.path.abspath(__file__))
src_dir = os.path.join(current_dir, '..', '..', '..')
sys.path.append(src_dir)

from data_preprocessing.data_access.s3 import S3DataAccess
from feature_engineering.base import TimeBarFeatureEngineering

# Configuration
CONFIG = {
    's3_region': 'us-east-1',
    's3_profile': 'blitvinfdp',
    's3_quotes_path': 's3://orderflowanalysis/intermediate/normalized/2024/02/28/level2q/AMERICAS/XCIS-20240228.parquet',
    'bar_duration_ms': 250,
}

# Selected high-activity periods from previous run
SELECTED_PERIODS = [
    {'ticker': 'URBN', 'datetime': '2024-02-28 15:14:52'},
    {'ticker': 'SQSP', 'datetime': '2024-02-28 14:48:39.750'},
    {'ticker': 'SQSP', 'datetime': '2024-02-28 14:48:40'},
    {'ticker': 'URBN', 'datetime': '2024-02-28 15:14:52.250'},
    {'ticker': 'URBN', 'datetime': '2024-02-28 15:35:13.750'},
    {'ticker': 'MBUU', 'datetime': '2024-02-28 15:29:07.500'},
    {'ticker': 'CLSK', 'datetime': '2024-02-28 14:31:36.500'},
    {'ticker': 'KOLD', 'datetime': '2024-02-28 13:37:51.250'},
    {'ticker': 'KOLD', 'datetime': '2024-02-28 13:33:36.750'},
]

def extract_l2q_test_data():
    """Extract raw L2Q data for selected periods and save as sorted CSV."""
    try:
        # Load data
        s3_access = S3DataAccess(region=CONFIG['s3_region'], profile_name=CONFIG['s3_profile'])
        raw_data = s3_access.read(CONFIG['s3_quotes_path'])
        
        # Add bar_id for filtering
        data_with_bars = TimeBarFeatureEngineering.bar_time_addition(
            raw_data, 'TimestampNanoseconds', CONFIG['bar_duration_ms']
        )
        
        # Build filter conditions for all selected periods
        filter_conditions = []
        for period in SELECTED_PERIODS:
            ticker = period['ticker']
            datetime_str = period['datetime']
            
            condition = (
                (pl.col('Ticker') == ticker) &
                (pl.col('ISOExchangeCode') == 'CIS') &
                (pl.col('bar_id_dt').dt.strftime('%Y-%m-%d %H:%M:%S%.3f').str.starts_with(datetime_str))
            )
            filter_conditions.append(condition)
        
        # Combine all conditions with OR
        combined_filter = filter_conditions[0]
        for condition in filter_conditions[1:]:
            combined_filter = combined_filter | condition
        
        # Filter and collect data
        filtered_data = data_with_bars.filter(combined_filter).collect()
        
        # Sort by Ticker, ISOExchangeCode, TimestampNanoseconds
        sorted_data = filtered_data.sort(['Ticker', 'ISOExchangeCode', 'TimestampNanoseconds'])
        
        print(f"Extracted {len(sorted_data)} L2Q quotes across {len(SELECTED_PERIODS)} periods")
        print(f"Tickers: {sorted_data['Ticker'].unique().to_list()}")
        print(f"Time range: {sorted_data['bar_id_dt'].min()} to {sorted_data['bar_id_dt'].max()}")
        
        # Save to CSV
        output_path = '/Users/blitvin/IdeaProjects/quant-research-sample-using-amazon-ecs-and-aws-batch/samples/order_flow_ray/src/tests/feature_engineering/l2_feed/test_data/raw_l2q_high_activity_periods.csv'
        sorted_data.write_csv(output_path)
        
        print(f"Raw L2Q test data saved to: {output_path}")
        
        # Show sample of data
        print("\nSample data (first 10 rows):")
        sample_cols = ['Ticker', 'ISOExchangeCode', 'bar_id_dt', 'BidPrice1', 'AskPrice1', 'BidQuantity1', 'AskQuantity1']
        print(sorted_data.select(sample_cols).head(10))
        
        return sorted_data
        
    except Exception as e:
        print(f"Error: {e}")
        return None

if __name__ == '__main__':
    extract_l2q_test_data()