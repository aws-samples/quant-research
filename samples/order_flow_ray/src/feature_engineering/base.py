# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: MIT-0

"""Base class for feature engineering."""
from abc import ABC, abstractmethod
from typing import Any, List
import polars as pl


class TimeBarFeatureEngineering:
    """Base class for feature engineering with configurable bar aggregation."""
    
    def __init__(self, raw_data: pl.LazyFrame, bar_duration_ms: int, timestamp_col: str):
        """Bar feature engineering initialization.
        
        Args:
            raw_data: Input dataframe
            bar_duration_ms: Bar duration in milliseconds
            timestamp_col: Name of timestamp column (nanoseconds)
        """
        self.raw_data = self.bar_time_addition(raw_data, timestamp_col, bar_duration_ms)
        self.bar_duration_ms = bar_duration_ms
        self.timestamp_col = timestamp_col
    
    @staticmethod
    def bar_time_addition(data: pl.LazyFrame, timestamp_col: str, bar_duration_ms: int) -> pl.LazyFrame:
        """Bar time column addition to data.
        
        Args:
            data: Input data with timestamp column
            timestamp_col: Name of timestamp column
            bar_duration_ms: Bar duration in milliseconds
            
        Returns:
            Data with bar_id column added
        """
        # Get schema to check column type
        schema = data.collect_schema()
        col_dtype = schema[timestamp_col]
        
        # Apply ceil(timestamp/bar_duration) * bar_duration with precision adjustments
        if col_dtype == pl.Datetime:
            # Convert datetime to milliseconds and apply formula
            timestamp_ms = pl.col(timestamp_col).dt.timestamp('ms')
            bar_id = ((timestamp_ms / bar_duration_ms).ceil() * bar_duration_ms).cast(pl.Int64)
        else:
            # Apply precision-adjusted formula based on timestamp magnitude
            bar_id = pl.when(
                pl.col(timestamp_col) > 1e16  # Nanoseconds (> 10^16)
            ).then(
                ((pl.col(timestamp_col) / 1_000_000 / bar_duration_ms).ceil() * bar_duration_ms).cast(pl.Int64)
            ).when(
                pl.col(timestamp_col) > 1e13  # Microseconds (> 10^13)
            ).then(
                ((pl.col(timestamp_col) / 1000 / bar_duration_ms).ceil() * bar_duration_ms).cast(pl.Int64)
            ).otherwise(
                ((pl.col(timestamp_col) / bar_duration_ms).ceil() * bar_duration_ms).cast(pl.Int64)  # Milliseconds
            )
        
        return data.with_columns([
            bar_id.alias('bar_id'),
            pl.from_epoch(bar_id, time_unit='ms').alias('bar_id_dt'),
            pl.lit(bar_duration_ms).alias('bar_duration_ms')
        ])
