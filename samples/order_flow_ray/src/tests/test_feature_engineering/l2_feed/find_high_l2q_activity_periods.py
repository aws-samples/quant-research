# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: MIT-0

"""Find 250ms periods with highest L2Q activity across multiple tickers during market hours."""
import os
import sys
import polars as pl

# Setup correct import path
try:
    current_dir = os.path.dirname(os.path.abspath(__file__))
except NameError:
    current_dir = '/Users/blitvin/IdeaProjects/quant-research-sample-using-amazon-ecs-and-aws-batch/samples/order_flow_ray/src/tests/feature_engineering/l2_feed'
src_dir = os.path.join(current_dir, '..', '..', '..')
sys.path.append(src_dir)

from data_preprocessing.data_access.s3 import S3DataAccess
from feature_engineering.base import TimeBarFeatureEngineering

# Configuration
CONFIG = {
    's3_region': 'us-east-1',
    's3_profile': 'blitvinfdp',
    's3_quotes_path': 's3://orderflowanalysis/intermediate/normalized/2024/02/28/level2q/AMERICAS/XCIS-20240228.parquet',
    'bar_duration_ms': 250,  # 250 milliseconds
}

def find_highest_l2q_activity_periods():
    """Find 250ms periods with highest L2Q quote counts across multiple tickers during market hours."""
    try:
        # Load data
        s3_access = S3DataAccess(region=CONFIG['s3_region'], profile_name=CONFIG['s3_profile'])
        raw_data = s3_access.read(CONFIG['s3_quotes_path'])
        
        # Add 250ms bar_id
        data_with_bars = TimeBarFeatureEngineering.bar_time_addition(
            raw_data, 'TimestampNanoseconds', CONFIG['bar_duration_ms']
        )
        
        # Filter for market hours (9:30-16:00 EST = 14:30-21:00 UTC on 2024-02-28)
        # 2024-02-28 14:30:00 UTC = 1709125800000000000 ns
        # 2024-02-28 21:00:00 UTC = 1709149200000000000 ns
        market_hours_data = data_with_bars.filter(
            (pl.col('TimestampNanoseconds') >= 1709125800000000000) &
            (pl.col('TimestampNanoseconds') <= 1709149200000000000)
        )
        
        # Group by ticker, bar to count quotes
        activity_summary = market_hours_data.group_by(['Ticker', 'ISOExchangeCode', 'bar_id', 'bar_id_dt']).agg([
            pl.len().alias('quote_count'),
            pl.col('BidPrice1').count().alias('bid_updates'),
            pl.col('AskPrice1').count().alias('ask_updates')
        ]).sort(['quote_count'], descending=True)
        
        result = activity_summary.collect()
        
        # Get top 20 periods to see variety of tickers
        top_periods = result.head(20)
        
        print("Top 20 250ms periods with highest L2Q activity during market hours:")
        print("=" * 80)
        print(top_periods.select(['Ticker', 'ISOExchangeCode', 'bar_id_dt', 'quote_count', 'bid_updates', 'ask_updates']))
        
        # Get unique tickers from top results
        unique_tickers = top_periods['Ticker'].unique().to_list()
        print(f"\nUnique tickers in top results: {unique_tickers}")
        
        # Select 10 periods across 2-5 different tickers
        selected_periods = []
        tickers_used = set()
        
        for row in top_periods.iter_rows(named=True):
            ticker = row['Ticker']
            if len(selected_periods) < 10 and (len(tickers_used) < 5 or ticker in tickers_used):
                selected_periods.append(row)
                tickers_used.add(ticker)
        
        print(f"\nSelected 10 periods across {len(tickers_used)} tickers:")
        print("=" * 60)
        for i, period in enumerate(selected_periods, 1):
            print(f"{i:2d}. {period['Ticker']:6s} {period['ISOExchangeCode']} {period['bar_id_dt']} - {period['quote_count']:,} quotes")
        
        return selected_periods
        
    except Exception as e:
        print(f"Error: {e}")
        return None

if __name__ == '__main__':
    find_highest_l2q_activity_periods()