from typing import Dict, Any, List, Callable
import polars as pl
from .base import DataNormalizer
from .normalized_schema import NormalizedSchema

class BMLLNormalizer(DataNormalizer):
    """BMLL data normalizer - pass-through since BMLL is our internal format."""
    
    def __init__(self):
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
    
    def rerun_failed_shards(self, get_failed_items: Callable[[List[Any]], List[Any]]) -> List[Any]:
        """Rerun processing for failed shards.
        
        Args:
            get_failed_items: Callback that takes results and returns items to rerun
            
        Returns:
            List of items that need reprocessing
        """
        # Default implementation: just call the callback
        # Subclasses can override for custom logic
        return get_failed_items