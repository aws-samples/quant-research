from abc import ABC, abstractmethod
from typing import Dict, Any, List, Callable
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
    
    @abstractmethod
    def rerun_failed_shards(self, get_failed_items: Callable[[List[Any]], List[Any]]) -> List[Any]:
        """Rerun processing for failed shards.
        
        Args:
            get_failed_items: Callback that takes results and returns items to rerun
            
        Returns:
            List of items that need reprocessing
        """
        pass