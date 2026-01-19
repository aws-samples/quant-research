from abc import ABC, abstractmethod
from typing import Dict, Any, List, Callable
import polars as pl

class DataNormalizer(ABC):
    """Base class for data source normalizers."""
    
    def __init__(self, max_retries: int = 3):
        self.files: List[tuple[str, int]] = []
        self.max_retries = max_retries
    
    @abstractmethod
    def normalize(self, raw_data: pl.LazyFrame, data_type: str) -> pl.LazyFrame:
        """Normalize raw data to internal format."""
        pass
    
    @abstractmethod
    def get_schema(self, data_type: str) -> Dict[str, Any]:
        """Get normalized schema for data type."""
        pass
    
    @abstractmethod
    def discover_files(self, data_access, raw_data_path: str, sort_order: str) -> List[tuple[str, int]]:
        """Discover files to process.
        
        Args:
            data_access: Data access instance
            raw_data_path: Base path to raw data
            sort_order: Sort order - 'asc' or 'desc'
            
        Returns:
            List of (file_path, file_size) tuples
        """
        pass
    
    @abstractmethod
    def get_failed_items(self, results: List[Any]) -> List[Any]:
        """Extract failed items from results for retry.
        
        Args:
            results: List of step execution results
            
        Returns:
            List of items that failed and should be retried
        """
        pass
    
    def rerun_failed_shards(self, get_failed_items: Callable[[List[Any]], List[Any]]) -> Callable[[List[Any]], List[tuple[str, int]]]:
        """Rerun processing for failed shards.
        
        Args:
            get_failed_items: Callback that takes results and returns items to rerun
            
        Returns:
            Callback function that processes results and returns failed files
        """
        return get_failed_items