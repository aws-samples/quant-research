"""Test L2Q feature engineering accuracy against expected results."""
import os
import sys
import polars as pl

# Setup import path
current_dir = os.path.dirname(os.path.abspath(__file__))
src_dir = os.path.join(current_dir, '..', '..', '..')
sys.path.append(src_dir)

from feature_engineering.order_flow import L2QFeatureEngineering

def test_l2q_sections_1_and_2():
    """Test L2Q feature engineering sections 1 and 2 against expected results."""
    
    # Load test data
    raw_data_path = os.path.join(current_dir, 'test_data', 'raw_l2q_high_activity_periods.csv')
    expected_data_path = os.path.join(current_dir, 'test_data', 'expected_l2q_high_activity_periods.csv')
    
    raw_df = pl.scan_csv(raw_data_path)
    expected_df = pl.read_csv(expected_data_path)
    
    print(f"Expected data shape: {expected_df.shape}")
    
    # Process with L2QFeatureEngineering
    feature_eng = L2QFeatureEngineering(bar_duration_ms=250)
    result_df = feature_eng.feature_computation(raw_df).collect()
    
    print(f"Result data shape: {result_df.shape}")
    
    # Test Section 1: Bar Metadata Features
    section1_cols = ['bar_id_dt', 'bar_duration_ms', 'market_state_mode']
    for col in section1_cols:
        if col in result_df.columns:
            print(f"✓ Section 1 - {col} present")
        else:
            print(f"✗ Section 1 - {col} missing")
    
    # Test Section 2: Quote Activity Features
    section2_cols = []
    for level in range(1, 11):
        section2_cols.extend([f'ask_update_count_l{level}', f'bid_update_count_l{level}'])
    
    for col in section2_cols:
        if col in result_df.columns:
            print(f"✓ Section 2 - {col} present")
        else:
            print(f"✗ Section 2 - {col} missing")
    
    # Validate each expected result
    for row in expected_df.iter_rows(named=True):
        ticker = row['Ticker']
        bar_id_dt = row['bar_id_dt']
        
        # Find matching row in results
        result_row = result_df.filter(
            (pl.col('Ticker') == ticker) & 
            (pl.col('bar_id_dt').dt.strftime('%Y-%m-%dT%H:%M:%S.%3f') == bar_id_dt)
        )
        
        if result_row.height == 0:
            print(f"✗ Missing result for {ticker} at {bar_id_dt}")
            continue
            
        result_row = result_row.row(0, named=True)
        
        # Check each section 2 field
        for col in section2_cols:
            if col in expected_df.columns:
                expected_val = row[col]
                actual_val = result_row.get(col, 'MISSING')
                if expected_val == actual_val:
                    print(f"✓ {ticker} {bar_id_dt} {col}: {actual_val}")
                else:
                    print(f"✗ {ticker} {bar_id_dt} {col}: expected {expected_val}, got {actual_val}")

if __name__ == '__main__':
    test_l2q_sections_1_and_2()