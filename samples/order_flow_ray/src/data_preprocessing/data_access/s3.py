import boto3
import polars as pl
from typing import Dict, Any, List, Tuple
from .base import DataAccess
import ray


@ray.remote
def _list_prefix_remote(region: str, profile_name: str, s3_path: str) -> List[Tuple[str, float]]:
    """Ray remote function to list files in a single S3 prefix.
    
    Args:
        region: AWS region
        profile_name: AWS profile name
        s3_path: Full S3 path (s3://bucket/prefix/)
        
    Returns:
        List of (file_path, size_gb) tuples
    """
    def _get_s3_client_internal():
        # Reuse the same pattern as _get_s3_client()
        if profile_name:
            session = boto3.Session(profile_name=profile_name)
        else:
            session = boto3.Session()
        return session.client('s3', region_name=region)
    
    s3_client = _get_s3_client_internal()
    
    # Parse s3://bucket/prefix
    path_parts = s3_path[5:].split('/', 1)
    bucket = path_parts[0]
    prefix = path_parts[1] if len(path_parts) > 1 else ''
    
    files = []
    paginator = s3_client.get_paginator('list_objects_v2')
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        if 'Contents' in page:
            for obj in page['Contents']:
                if not obj['Key'].endswith('/'):
                    files.append((f"s3://{bucket}/{obj['Key']}", obj['Size'] / (1024 ** 3)))
    return files

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
    
    def _discover_prefixes_sequential(self, bucket: str, base_prefix: str, parallel_discovery_threshold: int) -> List[str]:
        """Discover prefixes sequentially until threshold is reached.
        
        Args:
            bucket: S3 bucket name
            base_prefix: Base prefix to start discovery
            parallel_discovery_threshold: Stop when prefix count exceeds this
            
        Returns:
            List of S3 prefixes (full s3:// paths) to process in parallel
        """
        s3_client = self._get_s3_client()
        
        print(f"Starting sequential directory discovery...")
        
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
            depth += 1
            print(f"Depth {depth}: found {len(new_prefixes)} directories")
            if new_prefixes:
                print(f"  Sample directories: {new_prefixes[:3]}")
            if len(new_prefixes) >= parallel_discovery_threshold:
                prefixes = new_prefixes
                break
            prefixes = new_prefixes
        
        print(f"Sequential discovery complete: found {len(prefixes)} directories at depth {depth}")
        return [f"s3://{bucket}/{p}" for p in prefixes]
    
    def _list_files_parallel(self, prefixes: List[str]) -> List[Tuple[str, float]]:
        """List files in parallel across multiple prefixes using Ray.
        
        Args:
            prefixes: List of S3 prefixes (full s3:// paths) to process
            
        Returns:
            List of (file_path, size_gb) tuples
        """
        print(f"Starting parallel file listing across {len(prefixes)} directories...")
        print(f"Using region: {self.region}, profile_name: {self.profile_name}")
        
        futures = [_list_prefix_remote.remote(self.region, self.profile_name, prefix) for prefix in prefixes]
        results = ray.get(futures)
        return [file for sublist in results for file in sublist]
    
    def list_files_asynch(self, s3_path: str, parallel_discovery_threshold: int = 100) -> List[Tuple[str, float]]:
        """List files with parallel discovery when threshold is reached.
        
        Args:
            s3_path: Base S3 path
            parallel_discovery_threshold: Switch to parallel when prefix count exceeds this
        
        Returns:
            List of (file_path, size_gb) tuples
        """
        if not s3_path.startswith('s3://'):
            raise ValueError("Path must start with s3://")
        
        path_parts = s3_path[5:].split('/', 1)
        bucket = path_parts[0]
        base_prefix = path_parts[1].rstrip('/') + '/' if len(path_parts) > 1 else ''
        
        prefixes = self._discover_prefixes_sequential(bucket, base_prefix, parallel_discovery_threshold)
        return self._list_files_parallel(prefixes)
    
    def discover_files_asynch(self, s3_path: str, sort_order: str = 'asc', parallel_discovery_threshold: int = 100) -> List[Tuple[str, float]]:
        """Discover files with sorting - convenience wrapper around list_files_asynch.
        
        Args:
            s3_path: Base S3 path
            sort_order: Sort order - 'asc' or 'desc'
            parallel_discovery_threshold: Switch to parallel when prefix count exceeds this
            
        Returns:
            List of (file_path, size_gb) tuples sorted by size
        """
        print(f"Discovering files in: {s3_path}")
        files = self.list_files_asynch(s3_path, parallel_discovery_threshold)
        files.sort(key=lambda x: x[1], reverse=(sort_order == 'desc'))
        return files
    
    def write_inventory(self, inventory_name: str, files: List[Tuple[str, float]], metadata_path: str):
        """Write discovered files to inventory CSV."""
        df = pl.DataFrame({
            'file_path': [f[0] for f in files], 
            'file_size': [f[1] for f in files]
        })
        inventory_path = f"{metadata_path.rstrip('/')}/{inventory_name}_input_inventory.csv"
        self.write_csv(df, inventory_path)
        print(f"Wrote inventory to {inventory_path}")
        
        # Also write to local /tmp/ as fallback
        import os
        local_inventory_path = f"/tmp/{inventory_name}_input_inventory.csv"
        df.write_csv(local_inventory_path)
        print(f"Wrote local inventory to {local_inventory_path}")
    
    def read_inventory(self, inventory_name: str, metadata_path: str) -> List[Tuple[str, float]]:
        """Read discovered files from inventory CSV."""
        inventory_path = f"{metadata_path.rstrip('/')}/{inventory_name}_input_inventory.csv"
        try:
            storage_options = self._get_storage_options()
            df = pl.scan_csv(inventory_path, storage_options=storage_options)
            files = [(row['file_path'], float(row['file_size'])) for row in df.collect().to_dicts()]
            print(f"Read {len(files)} files from inventory {inventory_path}")
            return files
        except Exception as e:
            print(f"S3 inventory read failed: {e}, trying local fallback...")
            # Fallback to local /tmp/ file
            local_inventory_path = f"/tmp/{inventory_name}_input_inventory.csv"
            try:
                df = pl.scan_csv(local_inventory_path)
                files = [(row['file_path'], float(row['file_size'])) for row in df.collect().to_dicts()]
                print(f"Read {len(files)} files from local inventory {local_inventory_path}")
                return files
            except Exception as local_e:
                print(f"Local inventory read also failed: {local_e}")
                raise e  # Raise original S3 error
    
    def read(self, s3_path: str, **kwargs) -> pl.LazyFrame:
        """Read parquet from S3 path."""
        storage_options = self._get_storage_options()
        return pl.scan_parquet(s3_path, storage_options=storage_options, **kwargs)
    
    def write(self, data: pl.LazyFrame, s3_path: str, **kwargs) -> None:
        """Write parquet to S3 path."""
        storage_options = self._get_storage_options()
        data.sink_parquet(s3_path, storage_options=storage_options, **kwargs)
    
    def write_csv(self, data: pl.DataFrame, s3_path: str) -> None:
        """Write CSV to S3 path."""
        storage_options = self._get_storage_options()
        data.write_csv(s3_path, storage_options=storage_options)
    
    def get_storage_options(self) -> Dict[str, str]:
        """Get storage options for external use."""
        return self._get_storage_options()
    
    def group_files_by_size(self, files: List[Tuple[str, int]]) -> List[List[Tuple[str, int]]]:
        """Group files into subarrays where each subarray has total size approximately equal to max file size.
        
        Args:
            files: List of (file_path, file_size) tuples
            
        Returns:
            List of file groups, where each group's total size is approximately the max file size
        """
        if not files:
            return []
        
        max_size = max(size for _, size in files)
        
        groups = []
        current_group = []
        current_size = 0
        
        for file_path, file_size in files:
            if current_size + file_size > max_size and current_group:
                groups.append(current_group)
                current_group = []
                current_size = 0
            
            current_group.append((file_path, file_size))
            current_size += file_size
        
        if current_group:
            groups.append(current_group)
        
        return groups