# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: MIT-0

"""Find 5-minute periods with highest trade activity per exchange per symbol."""
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
    's3_trades_path': 's3://orderflowanalysis/intermediate/normalized/2024/02/28/trades/AMERICAS/trades-XASE-20240228.parquet',
    'bar_duration_ms': 300000,  # 5 minutes = 300,000 ms
}

def find_highest_activity_periods():
    """Find 5-minute periods with highest trade counts per exchange per symbol."""
    try:
        # Load data
        s3_access = S3DataAccess(region=CONFIG['s3_region'], profile_name=CONFIG['s3_profile'])
        raw_data = s3_access.read(CONFIG['s3_trades_path'])
        
        # Add 5-minute bar_id
        data_with_bars = TimeBarFeatureEngineering.bar_time_addition(
            raw_data, 'TradeTimestampNanoseconds', CONFIG['bar_duration_ms']
        )
        
        # Group by exchange, symbol, and 5-minute bar to count trades
        activity_summary = data_with_bars.group_by(['MIC', 'Ticker', 'bar_id', 'bar_id_dt']).agg([
            pl.len().alias('trade_count'),
            pl.col('Size').sum().alias('total_volume'),
            pl.col('Price').min().alias('price_low'),
            pl.col('Price').max().alias('price_high')
        ]).sort(['MIC', 'Ticker', 'trade_count'], descending=[False, False, True])
        
        result = activity_summary.collect()
        
        # Get top 100 periods overall sorted by activity
        top_periods = result.sort('trade_count', descending=True).head(100)
        
        print("Top 100 5-minute periods with highest trade activity (sorted by activity):")
        print("=" * 80)
        print(top_periods.select(['MIC', 'Ticker', 'bar_id_dt', 'trade_count', 'total_volume', 'price_low', 'price_high']))
        
        return top_periods
        
    except Exception as e:
        print(f"Error: {e}")
        return None

if __name__ == '__main__':
    find_highest_activity_periods()