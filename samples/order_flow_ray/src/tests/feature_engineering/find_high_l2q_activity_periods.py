"""Find 5-minute periods with highest L2Q activity per exchange per symbol."""
import os
import sys
import polars as pl

# Setup correct import path
try:
    current_dir = os.path.dirname(os.path.abspath(__file__))
except NameError:
    current_dir = '/Users/blitvin/IdeaProjects/quant-research-sample-using-amazon-ecs-and-aws-batch/samples/order_flow_ray/src/tests/feature_engineering'
src_dir = os.path.join(current_dir, '..', '..')
sys.path.append(src_dir)

from data_preprocessing.data_access.s3 import S3DataAccess
from feature_engineering.base import TimeBarFeatureEngineering

# Configuration
CONFIG = {
    's3_region': 'us-east-1',
    's3_profile': 'blitvinfdp',
    's3_quotes_path': 's3://orderflowanalysis/intermediate/normalized/2024/02/28/level2q/AMERICAS/XCIS-20240228.parquet',
    'bar_duration_ms': 300000,  # 5 minutes = 300,000 ms
}

def find_highest_l2q_activity_periods():
    """Find 5-minute periods with highest L2Q quote counts per exchange per symbol."""
    try:
        # Load data
        s3_access = S3DataAccess(region=CONFIG['s3_region'], profile_name=CONFIG['s3_profile'])
        raw_data = s3_access.read(CONFIG['s3_quotes_path'])
        
        # Add 5-minute bar_id
        data_with_bars = TimeBarFeatureEngineering.bar_time_addition(
            raw_data, 'TimestampNanoseconds', CONFIG['bar_duration_ms']
        )
        
        # Filter for specific high-activity sample: CIS SPY at 2024-02-28 14:45:00
        filtered_data = data_with_bars.filter(
            (pl.col('ISOExchangeCode') == 'CIS') &
            (pl.col('Ticker') == 'SPY') &
            (pl.col('bar_id_dt').dt.strftime('%Y-%m-%d %H:%M:%S') == '2024-02-28 14:45:00')
        )
        
        result = filtered_data.collect()
        
        print(f"Filtered L2Q data for CIS SPY at 2024-02-28 14:45:00: {len(result)} quotes")
        print("Sample data:")
        print(result.select(['ISOExchangeCode', 'Ticker', 'bar_id_dt', 'BidPrice1', 'AskPrice1', 'TimestampNanoseconds']).head(10))
        
        return result
        
    except Exception as e:
        print(f"Error: {e}")
        return None

if __name__ == '__main__':
    find_highest_l2q_activity_periods()