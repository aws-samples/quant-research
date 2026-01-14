"""Base class for model trainers."""
from abc import ABC, abstractmethod
from typing import Any
import polars as pl


class Trainer(ABC):
    """Abstract base class for model training."""
    
    @abstractmethod
    def train(self, features: pl.LazyFrame) -> Any:
        """Train model on features.
        
        Args:
            features: Feature data
            
        Returns:
            Trained model
        """
        pass
