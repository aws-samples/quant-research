"""Base class for feature engineers."""
from abc import ABC, abstractmethod
from typing import Any, List
import polars as pl


class FeatureEngineer(ABC):
    """Abstract base class for feature engineering."""
    
    def __init__(self, max_retries: int = 3):
        self.max_retries = max_retries
    
    @abstractmethod
    def engineer_features(self, data: pl.LazyFrame) -> pl.LazyFrame:
        """Engineer features from normalized data.
        
        Args:
            data: Normalized data
            
        Returns:
            Data with engineered features
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
