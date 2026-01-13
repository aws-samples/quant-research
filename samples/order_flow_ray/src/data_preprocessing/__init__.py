"""
Data Preprocessing Module

This module provides data normalization capabilities for converting various
data sources into a standardized internal format. Currently uses BMLL format
as the internal standard.

Usage:
    from data_preprocessing.data_normalizer import NormalizerFactory
    from data_preprocessing.data_access import DataAccessFactory
    
    # Create data access and normalizer
    data_access = DataAccessFactory.create("s3")
    normalizer = NormalizerFactory.create("bmll")
    
    # Read, normalize, and write data
    raw_data = data_access.read("s3://bucket/path/file.parquet")
    normalized_data = normalizer.normalize(raw_data, "trades")
    data_access.write(normalized_data, "s3://bucket/output/normalized.parquet")
"""

from .data_normalizer import DataNormalizer, BMLLNormalizer, NormalizerFactory
from .data_access import DataAccess, S3DataAccess, DataAccessFactory

__all__ = [
    "DataNormalizer", "BMLLNormalizer", "NormalizerFactory",
    "DataAccess", "S3DataAccess", "DataAccessFactory"
]