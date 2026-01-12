"""
Data Preprocessing Module

This module provides data normalization capabilities for converting various
data sources into a standardized internal format. Currently uses BMLL format
as the internal standard.

Usage:
    from data_preprocessing.data_normalizer import NormalizerFactory
    
    normalizer = NormalizerFactory.create("bmll")
    normalized_data = normalizer.normalize(raw_data, "trades")
"""

from .data_normalizer import DataNormalizer, BMLLNormalizer, NormalizerFactory

__all__ = ["DataNormalizer", "BMLLNormalizer", "NormalizerFactory"]