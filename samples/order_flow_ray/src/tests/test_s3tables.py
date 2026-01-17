"""Test S3 Tables integration - read local file and write to S3 Tables."""
import sys
import os

current_dir = os.getcwd()
src_dir = os.path.dirname(current_dir)
sys.path.append(src_dir)

import polars as pl
from data_preprocessing.data_access import DataAccessFactory


def test_s3tables_write():
    """Read local parquet file and write to S3 Tables."""
    
    # Path to S3 trade file from documentation
    s3_file = "s3://bmlldata/2024/01/02/trades/AMERICAS/trades-ARCX-20240102.parquet"
    
    # Read S3 file into LazyFrame using S3 data access
    print(f"Reading file: {s3_file}")
    s3_access = DataAccessFactory.create('s3', region='us-east-1', profile_name='blitvinfdp')
    df = s3_access.read(s3_file)
    
    # Add DataType and Region columns for partitioning
    df = df.with_columns([
        pl.lit('trades').alias('DataType'),
        pl.lit('AMERICAS').alias('Region')
    ])
    
    print(f"Schema: {df.collect_schema()}")
    print(f"Estimated rows: {df.select(pl.count()).collect().item()}")
    
    # Create S3 Tables data access
    s3tables = DataAccessFactory.create(
        's3tables',
        region='us-east-1',
        table_bucket_arn='arn:aws:s3tables:us-east-1:614393260192:bucket/order-flow-analysis-s3table',
        namespace='trading',
        profile_name='blitvinfdp'
    )
    
    # Setup: Create namespace
    s3tables.create_namespace()
    
    # Write to S3 Tables with partitioning
    table_name = 'trades_test'
    print(f"\nWriting to S3 Tables: {table_name}")
    s3tables.write(df, table_name, mode='overwrite', partition_by=['TradeDate', 'DataType', 'Region', 'ISOExchangeCode'])
    
    print(f"✓ Successfully wrote to S3 Tables")
    
    # Read back to verify
    print(f"\nReading back from S3 Tables...")
    df_read = s3tables.read(table_name)
    row_count = df_read.select(pl.count()).collect().item()
    
    print(f"✓ Read {row_count:,} rows from S3 Tables")
    print(f"✓ Schema: {df_read.collect_schema()}")


if __name__ == "__main__":
    test_s3tables_write()
    print("\n✓ Test completed!")
