import boto3
import polars as pl
from typing import Dict, Any, List, Tuple
from .base import DataAccess

class S3DataAccess(DataAccess):
    """S3 data access implementation using polars."""
    
    def __init__(self, region: str, profile_name: str = None):
        self.profile_name = profile_name
        self.region = region
        self._storage_options = None
        self._s3_client = None
    
    def _get_s3_client(self):
        """Get boto3 S3 client."""
        if self._s3_client is None:
            if self.profile_name:
                session = boto3.Session(profile_name=self.profile_name)
            else:
                session = boto3.Session()
            self._s3_client = session.client('s3', region_name=self.region)
        return self._s3_client
    
    def _get_storage_options(self) -> Dict[str, str]:
        """Get AWS credentials for polars S3 access."""
        if self._storage_options is None:
            if self.profile_name:
                session = boto3.Session(profile_name=self.profile_name)
            else:
                session = boto3.Session()
            credentials = session.get_credentials()
            
            self._storage_options = {
                "aws_region": self.region,
                "aws_access_key_id": credentials.access_key,
                "aws_secret_access_key": credentials.secret_key
            }
            
            if credentials.token:
                self._storage_options["aws_session_token"] = credentials.token
        return self._storage_options
    
    def list_files(self, s3_path: str) -> List[Tuple[str, float]]:
        """List all files recursively with sizes in GB."""
        # Parse S3 path
        if not s3_path.startswith('s3://'):
            raise ValueError("Path must start with s3://")
        
        path_parts = s3_path[5:].split('/', 1)
        bucket = path_parts[0]
        prefix = path_parts[1] if len(path_parts) > 1 else ''
        
        s3_client = self._get_s3_client()
        files = []
        
        paginator = s3_client.get_paginator('list_objects_v2')
        for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
            if 'Contents' in page:
                for obj in page['Contents']:
                    if not obj['Key'].endswith('/'):
                        size_gb = obj['Size'] / (1024 ** 3)
                        files.append((f"s3://{bucket}/{obj['Key']}", size_gb))
        
        return files
    
    def list_files_asynch(self, s3_path: str, parallel_discovery_threshold: int = 100) -> List[Tuple[str, float]]:
        """List files with parallel discovery when threshold is reached.
        
        Args:
            s3_path: Base S3 path
            parallel_discovery_threshold: Switch to parallel when prefix count exceeds this
        
        Returns:
            List of (file_path, size_gb) tuples
        """
        import asyncio
        
        if not s3_path.startswith('s3://'):
            raise ValueError("Path must start with s3://")
        
        path_parts = s3_path[5:].split('/', 1)
        bucket = path_parts[0]
        base_prefix = path_parts[1].rstrip('/') + '/' if len(path_parts) > 1 else ''
        
        s3_client = self._get_s3_client()
        
        # Discover prefixes until threshold is reached
        prefixes = [base_prefix]
        depth = 0
        while len(prefixes) < parallel_discovery_threshold:
            new_prefixes = []
            for prefix in prefixes:
                paginator = s3_client.get_paginator('list_objects_v2')
                for page in paginator.paginate(Bucket=bucket, Prefix=prefix, Delimiter='/'):
                    if 'CommonPrefixes' in page:
                        new_prefixes.extend([cp['Prefix'] for cp in page['CommonPrefixes']])
            if not new_prefixes:
                break
            prefixes = new_prefixes
            depth += 1
        
        print(f"Sequential discovery complete: found {len(prefixes)} directories at depth {depth}")
        print(f"Starting parallel file listing...")
        
        # List files in parallel
        async def _list_prefix(prefix: str) -> List[Tuple[str, float]]:
            loop = asyncio.get_event_loop()
            def _list():
                files = []
                paginator = s3_client.get_paginator('list_objects_v2')
                for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
                    if 'Contents' in page:
                        for obj in page['Contents']:
                            if not obj['Key'].endswith('/'):
                                files.append((f"s3://{bucket}/{obj['Key']}", obj['Size'] / (1024 ** 3)))
                return files
            return await loop.run_in_executor(None, _list)
        
        async def _list_all():
            tasks = [_list_prefix(prefix) for prefix in prefixes]
            results = await asyncio.gather(*tasks)
            return [file for sublist in results for file in sublist]
        
        return asyncio.run(_list_all())
    
    def read(self, s3_path: str, **kwargs) -> pl.LazyFrame:
        """Read parquet from S3 path."""
        storage_options = self._get_storage_options()
        return pl.scan_parquet(s3_path, storage_options=storage_options, **kwargs)
    
    def write(self, data: pl.LazyFrame, s3_path: str, **kwargs) -> None:
        """Write parquet to S3 path."""
        storage_options = self._get_storage_options()
        data.sink_parquet(s3_path, storage_options=storage_options, **kwargs)