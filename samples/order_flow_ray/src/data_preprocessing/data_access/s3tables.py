import boto3
import polars as pl
from typing import Dict, Any, List, Tuple
from .base import DataAccess
from pyiceberg.catalog import load_catalog


class S3TablesDataAccess(DataAccess):
    """S3 Tables data access using Polars native Iceberg support."""
    
    def __init__(self, region: str, table_bucket_arn: str, namespace: str = "default", profile_name: str = None):
        """Initialize S3 Tables access.
        
        Args:
            region: AWS region
            table_bucket_arn: S3 Tables bucket ARN
            namespace: Table namespace
            profile_name: Optional AWS profile name
        """
        self.region = region
        self.table_bucket_arn = table_bucket_arn
        self.namespace = namespace
        self.profile_name = profile_name
        self._catalog = None
    
    def _get_catalog(self):
        """Get PyIceberg catalog."""
        if self._catalog is None:
            if self.profile_name:
                session = boto3.Session(profile_name=self.profile_name)
            else:
                session = boto3.Session()
            
            credentials = session.get_credentials()
            
            catalog_config = {
                "type": "rest",
                "uri": f"https://s3tables.{self.region}.amazonaws.com/iceberg",
                "warehouse": self.table_bucket_arn,
                "rest.sigv4-enabled": "true",
                "rest.signing-name": "s3tables",
                "rest.signing-region": self.region,
                "py-io-impl": "pyiceberg.io.pyarrow.PyArrowFileIO",
                "s3.region": self.region,
                "s3.access-key-id": credentials.access_key,
                "s3.secret-access-key": credentials.secret_key,
            }
            
            if credentials.token:
                catalog_config["s3.session-token"] = credentials.token
            
            self._catalog = load_catalog("s3tables", **catalog_config)
        
        return self._catalog
    
    def create_namespace(self, namespace: str = None) -> None:
        """Create namespace if it doesn't exist.
        
        Args:
            namespace: Namespace to create (uses self.namespace if not provided)
        """
        catalog = self._get_catalog()
        ns = namespace or self.namespace
        try:
            catalog.create_namespace(ns)
        except Exception:
            pass  # Namespace already exists
    
    def read(self, table_name: str, **kwargs) -> pl.LazyFrame:
        """Read from S3 Tables Iceberg table using Polars.
        
        Args:
            table_name: Table name (without namespace)
            **kwargs: Additional scan options
        
        Returns:
            Polars LazyFrame
        """
        catalog = self._get_catalog()
        table = catalog.load_table(f"{self.namespace}.{table_name}")
        
        return pl.scan_iceberg(table, **kwargs)
    
    def write(self, data: pl.LazyFrame, table_name: str, mode: str, **kwargs) -> None:
        """Write to S3 Tables Iceberg table using Polars.
        
        Args:
            data: Polars LazyFrame to write
            table_name: Table name (without namespace)
            mode: Write mode - 'append' or 'overwrite'
            **kwargs: Additional write options
        """
        catalog = self._get_catalog()
        full_table_name = f"{self.namespace}.{table_name}"
        
        # Try to load existing table, create if doesn't exist
        try:
            table = catalog.load_table(full_table_name)
        except Exception:
            # Create table from schema
            df_sample = data.limit(1).collect()
            schema = df_sample.to_arrow().schema
            table = catalog.create_table(full_table_name, schema=schema)
        
        # Write using Polars (must collect - no sink_iceberg available)
        df = data.collect()
        df.write_iceberg(table, mode=mode, **kwargs)
