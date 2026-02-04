"""Pytest configuration for feature engineering tests."""
import os
import sys
import pytest
import polars as pl

# Setup correct import path
current_dir = os.getcwd()
src_dir = os.path.join(current_dir, 'samples/order_flow_ray/src')
sys.path.append(src_dir)


@pytest.fixture(scope="session")
def test_s3_path():
    """Test S3 path for normalized trades data."""
    return 's3://orderflowanalysis/intermediate/normalized/2024/02/28/trades/AMERICAS/trades-XASE-20240228.parquet'


@pytest.fixture
def mock_trades_data():
    """Mock trades data for testing without S3 dependency."""
    return pl.LazyFrame({
        'TradeTimestampNanoseconds': [
            1709136000000000000,  # 2024-02-28 16:00:00
            1709136001000000000,  # 2024-02-28 16:00:01
            1709136002000000000   # 2024-02-28 16:00:02
        ],
        'Price': [100.0, 101.0, 99.5],
        'Size': [100.0, 200.0, 150.0],
        'Ticker': ['XASE', 'XASE', 'XASE'],
        'AggressorSide': [1, -1, 1]
    })