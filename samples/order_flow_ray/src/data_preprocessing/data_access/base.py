# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: MIT-0

from abc import ABC, abstractmethod
from typing import Dict, Any
import polars as pl

class DataAccess(ABC):
    """Base class for data access implementations."""
    
    @abstractmethod
    def read(self, path: str, **kwargs) -> pl.LazyFrame:
        """Read data from path."""
        pass
    
    @abstractmethod
    def write(self, data: pl.LazyFrame, path: str, **kwargs) -> None:
        """Write data to path."""
        pass