"""Test aggregation calculations against expected results for specific test cases."""
import os
import sys
import polars as pl

# Setup correct import path
current_dir = os.path.dirname(os.path.abspath(__file__))
src_dir = os.path.join(current_dir, '..', '..', '..')
sys.path.append(src_dir)

from feature_engineering.order_flow import TradeFeatureEngineering

# Test configuration
TEST_CASES = [
    {'ticker': 'TELL', 'timeframe': '20240228_210000'},
    {'ticker': 'PTN', 'timeframe': '20240228_143500'},
    {'ticker': 'GSAT', 'timeframe': '20240228_210000'}
]

def test_aggregation_calculations():
    """Test that aggregation calculations match expected results."""
    test_data_dir = os.path.join(current_dir, 'test_data')
    
    fe = TradeFeatureEngineering(bar_duration_ms=300000)  # 5 minutes
    
    failed_tests = []
    
    for test_case in TEST_CASES:
        ticker = test_case['ticker']
        timeframe = test_case['timeframe']
        
        print(f"\n=== Testing {ticker} at {timeframe} ===")
        
        try:
            # Load raw data
            raw_data_path = os.path.join(test_data_dir, f'raw_trades_{ticker}_{timeframe}.csv')
            if not os.path.exists(raw_data_path):
                raise FileNotFoundError(f"Raw data file not found: {raw_data_path}")
            raw_data = pl.read_csv(raw_data_path).lazy()
            
            # Load expected results
            expected_path = os.path.join(test_data_dir, f'expected_results_{ticker}_{timeframe}.csv')
            if not os.path.exists(expected_path):
                raise FileNotFoundError(f"Expected results file not found: {expected_path}")
            expected_df = pl.read_csv(expected_path)
            
            # Calculate actual results
            actual_result = fe.feature_computation(raw_data)
            actual_df = actual_result.collect()
            
            print(f"Raw trades: {len(pl.read_csv(raw_data_path))}")
            print(f"Expected shape: {expected_df.shape}")
            print(f"Actual shape: {actual_df.shape}")
            
            # Compare key metrics
            key_columns = [
                'bar_id', 'trade_count', 'volume', 'price_open', 'price_high', 
                'price_low', 'price_close', 'vwap', 'volume_imbalance_ratio',
                'trade_imbalance_ratio', 'unassigned_volume_ratio', 'unassigned_count_ratio'
            ]
            
            matches = True
            for col in key_columns:
                if col in expected_df.columns and col in actual_df.columns:
                    expected_val = expected_df[col][0]
                    actual_val = actual_df[col][0]
                    
                    # Handle floating point comparison
                    if isinstance(expected_val, float) and isinstance(actual_val, float):
                        if abs(expected_val - actual_val) > 1e-6:
                            print(f"  ❌ {col}: Expected {expected_val}, Got {actual_val}")
                            matches = False
                        else:
                            print(f"  ✅ {col}: {actual_val}")
                    else:
                        if expected_val != actual_val:
                            print(f"  ❌ {col}: Expected {expected_val}, Got {actual_val}")
                            matches = False
                        else:
                            print(f"  ✅ {col}: {actual_val}")
            
            if matches:
                print(f"  🎉 All calculations match for {ticker}!")
            else:
                print(f"  ⚠️  Some calculations don't match for {ticker}")
                failed_tests.append(f"{ticker}_{timeframe}")
                
        except Exception as e:
            print(f"  ❌ Error testing {ticker}: {e}")
            failed_tests.append(f"{ticker}_{timeframe}")
    
    if failed_tests:
        print(f"\n❌ FAILED: {len(failed_tests)} test(s) failed: {', '.join(failed_tests)}")
        sys.exit(1)
    else:
        print("\n✅ All tests passed!")
        sys.exit(0)

if __name__ == '__main__':
    test_aggregation_calculations()