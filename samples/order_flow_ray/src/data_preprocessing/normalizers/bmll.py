from typing import Dict, Any
import polars as pl
from .base import DataNormalizer
from .normalized_schema import NormalizedSchema

class BMLLNormalizer(DataNormalizer):
    """BMLL data normalizer - pass-through since BMLL is our internal format."""
    
    def __init__(self):
        self.schema = NormalizedSchema()
    
    def normalize(self, raw_data: pl.LazyFrame, data_type: str) -> pl.LazyFrame:
        """Pass-through normalization for BMLL data."""
        return raw_data


    def get_schema(self, data_type: str) -> Dict[str, Any]:
        """Get BMLL schema for data type."""
        return self.schema.get_schema(data_type)