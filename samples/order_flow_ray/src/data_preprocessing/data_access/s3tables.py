# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: MIT-0

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
    
    @staticmethod
    def _infer_schema_from_polars(df: pl.DataFrame):
        """Convert Polars DataFrame to Iceberg schema.
        
        Args:
            df: Polars DataFrame
            
        Returns:
            Iceberg Schema
        """
        from pyiceberg.schema import Schema as IcebergSchema
        from pyiceberg.types import (
            NestedField, StringType, LongType, IntegerType, 
            DoubleType, BooleanType, DateType, TimestampType
        )
        import pyarrow as pa
        
        arrow_table = df.to_arrow()
        
        # Map Arrow types to Iceberg types
        type_map = {
            pa.large_string(): StringType(),
            pa.string(): StringType(),
            pa.int64(): LongType(),
            pa.int32(): IntegerType(),
            pa.int8(): IntegerType(),
            pa.float64(): DoubleType(),
            pa.bool_(): BooleanType(),
            pa.date32(): DateType(),
        }
        
        fields = []
        for i, field in enumerate(arrow_table.schema):
            # Map Arrow type to Iceberg type
            if pa.types.is_timestamp(field.type):
                iceberg_type = TimestampType()
            else:
                iceberg_type = type_map.get(field.type, StringType())
            
            fields.append(NestedField(
                field_id=i+1,
                name=field.name,
                field_type=iceberg_type,
                required=not field.nullable
            ))
        
        return IcebergSchema(*fields)
    
    def write(self, data: pl.LazyFrame, table_name: str, mode: str, partition_by: list[str] = None, **kwargs) -> None:
        """Write to S3 Tables Iceberg table using Polars.
        
        Args:
            data: Polars LazyFrame to write
            table_name: Table name (without namespace)
            mode: Write mode - 'append' or 'overwrite'
            partition_by: List of columns to partition by
            **kwargs: Additional write options
        """
        from pyiceberg.partitioning import PartitionSpec, PartitionField
        from pyiceberg.transforms import YearTransform, MonthTransform, DayTransform, IdentityTransform
        from pyiceberg.schema import Schema as IcebergSchema
        
        catalog = self._get_catalog()
        full_table_name = f"{self.namespace}.{table_name}"
        
        # Try to load existing table
        try:
            table = catalog.load_table(full_table_name)
        except Exception:
            # Table doesn't exist - create with partition spec
            df = data.collect()
            iceberg_schema = self._infer_schema_from_polars(df)
            
            # Build partition spec using Iceberg schema field IDs - 6 levels
            partition_spec = None
            if partition_by:
                fields = []
                field_id_counter = 1000
                
                # 6-level partitioning: Year, Month, Day, DataType, Region, ISOExchangeCode
                for col in ['Year', 'Month', 'Day', 'DataType', 'Region', 'ISOExchangeCode']:
                    if col in [f.name for f in iceberg_schema.fields]:
                        col_field = iceberg_schema.find_field(col)
                        fields.append(PartitionField(
                            source_id=col_field.field_id,
                            field_id=field_id_counter,
                            transform=IdentityTransform(),
                            name=col
                        ))
                        field_id_counter += 1
                
                partition_spec = PartitionSpec(*fields) if fields else None
            
            try:
                table = catalog.create_table(full_table_name, schema=iceberg_schema, partition_spec=partition_spec)
            except Exception:
                # Another worker created it, reload
                table = catalog.load_table(full_table_name)
        
        # Write using Polars
        if not isinstance(data, pl.DataFrame):
            df = data.collect()
        else:
            df = data
            
        df.write_iceberg(table, mode=mode, **kwargs)
