# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: MIT-0

"""Base class for model inference."""
from abc import ABC, abstractmethod
from typing import Any
import polars as pl


class Predictor(ABC):
    """Abstract base class for model inference."""
    
    @abstractmethod
    def predict(self, features: pl.LazyFrame, model: Any) -> pl.LazyFrame:
        """Generate predictions using trained model.
        
        Args:
            features: Feature data
            model: Trained model
            
        Returns:
            Predictions
        """
        pass
