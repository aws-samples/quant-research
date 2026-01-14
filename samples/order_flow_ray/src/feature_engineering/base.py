"""Base class for feature engineers."""
from abc import ABC, abstractmethod
import polars as pl


class FeatureEngineer(ABC):
    """Abstract base class for feature engineering."""
    
    @abstractmethod
    def engineer_features(self, data: pl.LazyFrame) -> pl.LazyFrame:
        """Engineer features from normalized data.
        
        Args:
            data: Normalized data
            
        Returns:
            Data with engineered features
        """
        pass
