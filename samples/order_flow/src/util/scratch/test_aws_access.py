#!/usr/bin/env python3
"""
Test AWS credentials and different authentication approaches
"""

import polars as pl
import boto3
import logging
import os

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def test_aws_access():
    """Test AWS access with different approaches."""
    
    # Check AWS credentials
    logger.info("Checking AWS credentials...")
    try:
        session = boto3.Session(profile_name='blitvinfdp')
        credentials = session.get_credentials()
        logger.info(f"✓ AWS credentials found for profile 'blitvinfdp'")
        logger.info(f"  Access Key: {credentials.access_key[:10]}...")
        logger.info(f"  Region: {session.region_name}")
    except Exception as e:
        logger.error(f"AWS credentials issue: {e}")
    
    # Test S3 access with boto3
    logger.info("\nTesting S3 access with boto3...")
    try:
        s3_client = boto3.Session(profile_name='blitvinfdp').client('s3')
        response = s3_client.list_objects_v2(Bucket='bmlldata', Prefix='2024/01/02/level2q/', MaxKeys=5)
        logger.info(f"✓ boto3 S3 access works: {len(response.get('Contents', []))} objects found")
        
        # Test head_object on level2q file
        logger.info("Listing actual level2q files...")
        response = s3_client.list_objects_v2(Bucket='bmlldata', Prefix='2024/01/02/level2q/AMERICAS/', MaxKeys=5)
        logger.info(f"✓ Found {len(response.get('Contents', []))} level2q files:")
        for obj in response.get('Contents', []):
            logger.info(f"  {obj['Key']}")
        
    except Exception as e:
        logger.error(f"boto3 S3 access failed: {e}")
    
    # Test polars with explicit credentials
    logger.info("\nTesting polars with explicit credentials...")
    s3_path = "s3://bmlldata/2024/02/02/trades/AMERICAS/trades-EDGA-20240202.parquet"
    
    try:
        # Get credentials from boto3 session
        session = boto3.Session(profile_name='blitvinfdp')
        credentials = session.get_credentials()
        
        # Pass credentials to polars storage_options
        storage_options = {
            "aws_region": "us-east-1",
            "aws_access_key_id": credentials.access_key,
            "aws_secret_access_key": credentials.secret_key
        }
        
        if credentials.token:
            storage_options["aws_session_token"] = credentials.token
        
        df = pl.scan_parquet(s3_path, storage_options=storage_options)
        schema = df.collect_schema()
        logger.info(f"✓ polars scan works with explicit credentials: {len(schema)} columns")
        
    except Exception as e:
        logger.error(f"polars scan failed: {e}")

if __name__ == "__main__":
    test_aws_access()