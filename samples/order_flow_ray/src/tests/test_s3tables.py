# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: MIT-0

"""Test S3 write - read and write to S3."""
import sys
import os

current_dir = os.getcwd()
src_dir = os.path.dirname(current_dir)
sys.path.append(src_dir)

import polars as pl
from data_preprocessing.data_access import DataAccessFactory


def test_s3tables_write():
    """Read S3 file and write to S3."""
    
    # Path to S3 trade file from documentation
    s3_file = "s3://bmlldata/2024/01/02/trades/AMERICAS/trades-ARCX-20240102.parquet"
    
    # Read S3 file into LazyFrame using S3 data access
    print(f"Reading file: {s3_file}")
    s3_access = DataAccessFactory.create('s3', region='us-east-1', profile_name='blitvinfdp')
    df = s3_access.read(s3_file).limit(1000)
    
    # Add DataType, Region, Year, Month, Day columns for partitioning
    df = df.with_columns([
        pl.lit('trades').alias('DataType'),
        pl.lit('AMERICAS').alias('Region'),
        pl.col('TradeDate').dt.year().cast(pl.Int32).alias('Year'),
        pl.col('TradeDate').dt.month().cast(pl.Int8).alias('Month'),
        pl.col('TradeDate').dt.day().cast(pl.Int8).alias('Day')
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
    
    # Drop table if exists using AWS SDK
    import boto3
    s3tables_client = boto3.client('s3tables', region_name='us-east-1')
    try:
        s3tables_client.delete_table(
            tableBucketARN='arn:aws:s3tables:us-east-1:614393260192:bucket/order-flow-analysis-s3table',
            namespace='trading',
            name=table_name
        )
        print(f'✓ Dropped existing table {table_name}')
    except s3tables_client.exceptions.NotFoundException:
        print(f'Table {table_name} does not exist')
    except Exception as e:
        print(f'Could not drop table: {e}')
    print(f"\nWriting to S3 Tables: {table_name}")
    s3tables.write(df, table_name, mode='overwrite', partition_by=['Year', 'Month', 'Day', 'DataType', 'Region', 'ISOExchangeCode'])
    
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
