import boto3
import polars as pl
from typing import Dict, Any
from .base import DataAccess

class S3DataAccess(DataAccess):
    """S3 data access implementation using polars."""
    
    def __init__(self, profile_name: str = "blitvinfdp", region: str = "us-east-1"):
        self.profile_name = profile_name
        self.region = region
        self._storage_options = None
    
    def _get_storage_options(self) -> Dict[str, str]:
        """Get AWS credentials for polars S3 access."""
        if self._storage_options is None:
            session = boto3.Session(profile_name=self.profile_name)
            credentials = session.get_credentials()
            
            self._storage_options = {
                "aws_region": self.region,
                "aws_access_key_id": credentials.access_key,
                "aws_secret_access_key": credentials.secret_key
            }
            
            if credentials.token:
                self._storage_options["aws_session_token"] = credentials.token
        
        return self._storage_options
    
    def read(self, s3_path: str, **kwargs) -> pl.LazyFrame:
        """Read parquet from S3 path."""
        storage_options = self._get_storage_options()
        return pl.scan_parquet(s3_path, storage_options=storage_options, **kwargs)
    
    def write(self, data: pl.LazyFrame, s3_path: str, **kwargs) -> None:
        """Write parquet to S3 path."""
        storage_options = self._get_storage_options()
        data.collect().write_parquet(s3_path, storage_options=storage_options, **kwargs)