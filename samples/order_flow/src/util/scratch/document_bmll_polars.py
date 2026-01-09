#!/usr/bin/env python3
"""
Document BMLL File Structure using polars with explicit credentials
"""

import polars as pl
import boto3
import json
from datetime import datetime
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def document_bmll_structure():
    """Document BMLL file structure using polars with explicit credentials."""
    
    bucket_name = "bmlldata"
    sample_date = "2024/01/02"
    
    # Get AWS credentials
    session = boto3.Session(profile_name='blitvinfdp')
    credentials = session.get_credentials()
    s3_client = session.client('s3')
    
    storage_options = {
        "aws_region": "us-east-1",
        "aws_access_key_id": credentials.access_key,
        "aws_secret_access_key": credentials.secret_key
    }
    
    if credentials.token:
        storage_options["aws_session_token"] = credentials.token
    
    # First, discover available data types
    logger.info(f"Discovering data types for {sample_date}...")
    response = s3_client.list_objects_v2(
        Bucket=bucket_name,
        Prefix=f"{sample_date}/",
        Delimiter='/',
        MaxKeys=50
    )
    
    data_types = []
    if 'CommonPrefixes' in response:
        for prefix in response['CommonPrefixes']:
            data_type = prefix['Prefix'].replace(f"{sample_date}/", '').rstrip('/')
            data_types.append(data_type)
            logger.info(f"  Found data type: {data_type}")
    
    # For each data type, find actual files
    logger.info("Finding actual files for each data type...")
    actual_files = {}
    for data_type in data_types:
        files_response = s3_client.list_objects_v2(
            Bucket=bucket_name,
            Prefix=f"{sample_date}/{data_type}/AMERICAS/",
            MaxKeys=5
        )
        
        if 'Contents' in files_response:
            actual_files[data_type] = [obj['Key'] for obj in files_response['Contents']]
            logger.info(f"  {data_type}: {len(actual_files[data_type])} files found")
        else:
            actual_files[data_type] = []
            logger.info(f"  {data_type}: No files found")
    
    results = {
        'documentation_info': {
            'date': sample_date,
            'timestamp': datetime.now().isoformat(),
            'discovered_data_types': data_types,
            'actual_files': actual_files
        },
        'directory_structure_analysis': {
            'bucket': bucket_name,
            'pattern': 'YYYY/MM/DD/{data_type}/AMERICAS/{filename}',
            'naming_conventions': {
                'trades': 'trades-{EXCHANGE}-YYYYMMDD.parquet',
                'reference': 'reference-{EXCHANGE}-YYYYMMDD.parquet', 
                'level2q': '{EXCHANGE}-YYYYMMDD.parquet'
            },
            'exchanges_found': list(set([
                file.split('/')[-1].split('-')[1 if 'trades' in file or 'reference' in file else 0].split('-')[0]
                for files in actual_files.values() 
                for file in files
                if 'AMERICAS' in file
            ])),
            'sample_paths': {
                data_type: actual_files[data_type][:3] if actual_files[data_type] else []
                for data_type in data_types
            }
        },
        'file_structures': {}
    }
    
    # Document structure for each data type with actual files
    for data_type in data_types:
        if not actual_files[data_type]:
            logger.info(f"Skipping {data_type} - no files found")
            continue
            
        logger.info(f"Documenting {data_type} structure...")
        
        try:
            # Use first available file
            s3_key = actual_files[data_type][0]
            s3_path = f"s3://{bucket_name}/{s3_key}"
            
            logger.info(f"Scanning: {s3_path}")
            
            # Use polars with explicit credentials
            df = pl.scan_parquet(s3_path, storage_options=storage_options)
            schema = df.collect_schema()
            
            # Document structure
            file_structure = {
                'file_info': {
                    's3_path': s3_path,
                    'total_columns': len(schema),
                    'sample_file': s3_key
                },
                'complete_schema': {name: str(dtype) for name, dtype in schema.items()},
                'column_list': list(schema.keys())
            }
            
            results['file_structures'][data_type] = file_structure
            
            logger.info(f"✓ {data_type}: {len(schema)} columns")
            
        except Exception as e:
            logger.error(f"Failed to document {data_type}: {e}")
            results['file_structures'][data_type] = {'error': str(e)}
    
    # Save results
    output_file = f"../../docs/bmll_data/bmll_structure_documentation.json"
    
    with open(output_file, 'w') as f:
        json.dump(results, f, indent=2, default=str)
    
    logger.info(f"✓ Structure documentation saved to: {output_file}")
    
    return results

if __name__ == "__main__":
    document_bmll_structure()