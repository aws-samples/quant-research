# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: MIT-0

"""Find available L2 quote files in S3."""
import os
import sys
import boto3

# Setup correct import path
current_dir = os.path.dirname(os.path.abspath(__file__))
src_dir = os.path.join(current_dir, '..', '..', '..')
sys.path.append(src_dir)

from data_preprocessing.data_access.s3 import S3DataAccess
import polars as pl

def find_l2_quotes_files():
    """Find available L2 quote files in S3."""
    try:
        # Create S3 client
        session = boto3.Session(profile_name='blitvinfdp')
        s3 = session.client('s3', region_name='us-east-1')
        
        bucket = 'orderflowanalysis'
        prefix = 'intermediate/normalized/2024/02/28/'
        
        # List objects with the prefix
        response = s3.list_objects_v2(Bucket=bucket, Prefix=prefix)
        
        if 'Contents' in response:
            # Filter for quote files only, exclude trades
            quote_files = []
            for obj in response['Contents']:
                key = obj['Key']
                size = obj['Size']
                if ('quote' in key.lower() or 'l2' in key.lower()) and 'trade' not in key.lower():
                    quote_files.append({'path': f's3://{bucket}/{key}', 'size': size})
            
            # Sort by size
            quote_files.sort(key=lambda x: x['size'])
            
            print("Quote files sorted by size:")
            s3_access = S3DataAccess(region='us-east-1', profile_name='blitvinfdp')
            
            for file_info in quote_files:
                print(f"  {file_info['path']} ({file_info['size']:,} bytes)")
                
                # Test with S3DataAccess to find smallest with >0 records
                try:
                    df = s3_access.read(file_info['path'])
                    count = df.select(pl.len()).collect().item()
                    print(f"    Records: {count:,}")
                    
                    if count > 0:
                        print(f"    ✓ Found smallest non-empty file: {file_info['path']}")
                        return file_info['path']
                        
                except Exception as e:
                    print(f"    ❌ Error scanning: {e}")
        else:
            print("No files found with the prefix")
            
    except Exception as e:
        print(f"Error: {e}")
    
    return None

if __name__ == '__main__':
    find_l2_quotes_files()