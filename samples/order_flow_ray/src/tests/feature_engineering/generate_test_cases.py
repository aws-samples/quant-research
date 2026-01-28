"""Extract specific ticker/timeframe data and generate expected aggregation results."""
import os
import sys
import polars as pl

# Setup correct import path
current_dir = os.path.dirname(os.path.abspath(__file__))
src_dir = os.path.join(current_dir, '..', '..')
sys.path.append(src_dir)

from data_preprocessing.data_access.s3 import S3DataAccess
from feature_engineering.base import TimeBarFeatureEngineering
from feature_engineering.order_flow import TradeFeatureEngineering

# Configuration
CONFIG = {
    's3_region': 'us-east-1',
    's3_profile': 'blitvinfdp',
    's3_trades_path': 's3://orderflowanalysis/intermediate/normalized/2024/02/28/trades/AMERICAS/trades-XASE-20240228.parquet',
    'bar_duration_ms': 300000,  # 5 minutes
    'test_cases': [
        {'ticker': 'TELL', 'timeframe': '2024-02-28 21:00:00'},
        {'ticker': 'PTN', 'timeframe': '2024-02-28 14:35:00'},
        {'ticker': 'GSAT', 'timeframe': '2024-02-28 21:00:00'}
    ]
}

def extract_test_data():
    """Extract raw trade data for specific tickers and timeframes."""
    try:
        # Load data
        s3_access = S3DataAccess(region=CONFIG['s3_region'], profile_name=CONFIG['s3_profile'])
        raw_data = s3_access.read(CONFIG['s3_trades_path'])
        
        # Add bar_id for filtering
        data_with_bars = TimeBarFeatureEngineering.bar_time_addition(
            raw_data, 'TradeTimestampNanoseconds', CONFIG['bar_duration_ms']
        )
        
        # Filter for specific test cases
        test_data_list = []
        for test_case in CONFIG['test_cases']:
            ticker = test_case['ticker']
            timeframe = test_case['timeframe']
            
            # Filter data
            filtered_data = data_with_bars.filter(
                (pl.col('Ticker') == ticker) &
                (pl.col('bar_id_dt').dt.strftime('%Y-%m-%d %H:%M:%S') == timeframe)
            ).collect()
            
            if len(filtered_data) > 0:
                print(f"\n{ticker} at {timeframe}: {len(filtered_data)} trades")
                test_data_list.append(filtered_data)
                
                # Save raw data for this test case
                output_path = f'/Users/blitvin/IdeaProjects/quant-research-sample-using-amazon-ecs-and-aws-batch/samples/order_flow_ray/src/tests/feature_engineering/test_data/raw_trades_{ticker}_{timeframe.replace(":", "").replace(" ", "_").replace("-", "")}.csv'
                filtered_data.write_csv(output_path)
                print(f"Raw data saved to: {output_path}")
            else:
                print(f"\n{ticker} at {timeframe}: No trades found")
        
        return test_data_list
        
    except Exception as e:
        print(f"Error: {e}")
        return None

def generate_expected_results():
    """Generate expected aggregation results for test cases."""
    try:
        # Extract test data first
        test_data_list = extract_test_data()
        
        if not test_data_list:
            print("No test data found")
            return
        
        # Process each test case through feature engineering
        fe = TradeFeatureEngineering(bar_duration_ms=CONFIG['bar_duration_ms'])
        
        for i, test_data in enumerate(test_data_list):
            test_case = CONFIG['test_cases'][i]
            ticker = test_case['ticker']
            timeframe = test_case['timeframe']
            
            print(f"\n=== Expected Results for {ticker} at {timeframe} ===")
            
            # Convert back to LazyFrame and process
            lazy_data = pl.LazyFrame(test_data)
            result = fe.feature_computation(lazy_data)
            expected_df = result.collect()
            
            print(f"Expected aggregated result:")
            print(expected_df)
            
            # Save expected results
            output_path = f'/Users/blitvin/IdeaProjects/quant-research-sample-using-amazon-ecs-and-aws-batch/samples/order_flow_ray/src/tests/feature_engineering/test_data/expected_results_{ticker}_{timeframe.replace(":", "").replace(" ", "_").replace("-", "")}.csv'
            expected_df.write_csv(output_path)
            print(f"Expected results saved to: {output_path}")
            
    except Exception as e:
        print(f"Error generating expected results: {e}")

if __name__ == '__main__':
    generate_expected_results()