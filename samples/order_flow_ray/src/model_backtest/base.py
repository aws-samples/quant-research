"""Base class for backtesting."""
from abc import ABC, abstractmethod
from typing import Any
import polars as pl


class Backtester(ABC):
    """Abstract base class for backtesting."""
    
    @abstractmethod
    def backtest(self, predictions: pl.LazyFrame) -> Any:
        """Run backtest on predictions.
        
        Args:
            predictions: Model predictions
            
        Returns:
            Backtest results
        """
        pass
