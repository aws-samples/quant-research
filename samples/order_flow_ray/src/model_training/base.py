"""Base class for model trainers."""
from abc import ABC, abstractmethod
from typing import Any, List
import polars as pl


class Trainer(ABC):
    """Abstract base class for model training."""
    
    def __init__(self, max_retries: int = 3):
        self.max_retries = max_retries
    
    @abstractmethod
    def train(self, features: pl.LazyFrame) -> Any:
        """Train model on features.
        
        Args:
            features: Feature data
            
        Returns:
            Trained model
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
