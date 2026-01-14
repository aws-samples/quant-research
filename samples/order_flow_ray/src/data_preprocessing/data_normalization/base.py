from abc import ABC, abstractmethod
from typing import Dict, Any
import polars as pl

class DataNormalizer(ABC):
    """Base class for data source normalizers."""
    
    @abstractmethod
    def normalize(self, raw_data: pl.LazyFrame, data_type: str) -> pl.LazyFrame:
        """Normalize raw data to internal format."""
        pass
    
    @abstractmethod
    def get_schema(self, data_type: str) -> Dict[str, Any]:
        """Get normalized schema for data type."""
        pass