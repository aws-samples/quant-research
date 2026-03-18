# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: MIT-0

from typing import Dict, Any, List, Callable
import polars as pl
from .base import DataNormalizer
from .normalized_schema import NormalizedSchema

class BMLLNormalizer(DataNormalizer):
    """BMLL data normalizer - pass-through since BMLL is our internal format."""
    
    def __init__(self, max_retries: int = 3):
        super().__init__(max_retries)
        self.schema = NormalizedSchema()
    
    def normalize(self, raw_data: pl.LazyFrame, data_type: str, source_path: str = None) -> pl.LazyFrame:
        """Add DataType, Region, Year, Month, Day columns."""
        df = raw_data
        
        if source_path:
            # Extract from path: YYYY/MM/DD/{data_type}/AMERICAS/{filename}
            parts = source_path.split('/')
            data_type_from_path = parts[-3] if len(parts) >= 3 else data_type
            region = parts[-2] if len(parts) >= 2 else 'AMERICAS'
            
            df = df.with_columns([
                pl.lit(data_type_from_path).alias('DataType'),
                pl.lit(region).alias('Region')
            ])
            data_type = data_type_from_path
        else:
            df = df.with_columns([
                pl.lit(data_type).alias('DataType'),
                pl.lit('AMERICAS').alias('Region')
            ])
        
        # Use appropriate date column based on data type
        date_col = 'Date' if data_type == 'reference' else 'TradeDate'
        
        # Add year, month, day columns for partitioning
        df = df.with_columns([
            pl.col(date_col).dt.year().cast(pl.Int32).alias('Year'),
            pl.col(date_col).dt.month().cast(pl.Int8).alias('Month'),
            pl.col(date_col).dt.day().cast(pl.Int8).alias('Day')
        ])
        
        return df

    def get_schema(self, data_type: str) -> Dict[str, Any]:
        """Get BMLL schema for data type."""
        return self.schema.get_schema(data_type)
    
    def discover_files(self, data_access, raw_data_path: str, sort_order: str) -> List[tuple[str, int]]:
        """Discover BMLL files to process.
        
        Args:
            data_access: Data access instance
            raw_data_path: Base path to raw data
            sort_order: Sort order - 'asc' or 'desc'
            
        Returns:
            List of (file_path, file_size) tuples sorted by size
        """
        files = data_access.list_files(raw_data_path)
        files = [(path, size) for path, size in files if '/reference/' not in path]
        files.sort(key=lambda x: x[1], reverse=(sort_order == 'desc'))
        self.files = files
        return files
    
    def get_failed_items(self, results: List[Any]) -> List[tuple[str, int]]:
        """Extract failed files from normalization results.
        
        Args:
            results: List of normalization results
            
        Returns:
            List of (file_path, file_size) tuples that failed
        """
        failed = [r for r in results if r['message'] != 'success']
        return [(r['input_path'], int(r['size_gb'] * (1024**3))) for r in failed]