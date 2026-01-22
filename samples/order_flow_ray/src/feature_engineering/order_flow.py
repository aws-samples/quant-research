"""Feature engineering with bar aggregation."""
from abc import ABC, abstractmethod
from typing import Any, List
import polars as pl
from .base import TimeBarFeatureEngineering


class FeatureEngineering(ABC):
    """Base class for feature engineering with configurable bar aggregation."""
    
    def __init__(self, bar_duration_ms: int = 1000, max_retries: int = 3):
        """Feature engineering initialization.
        
        Args:
            bar_duration_ms: Bar duration in milliseconds
            max_retries: Maximum retry attempts
        """
        self.bar_duration_ms = bar_duration_ms
        self.max_retries = max_retries
    
    @abstractmethod
    def feature_computation(self, data: pl.LazyFrame) -> pl.LazyFrame:
        """Feature computation from normalized data.
        
        Args:
            data: Normalized data
            
        Returns:
            Data with computed features
        """
        pass
    
    @abstractmethod
    def failure_extraction(self, results: List[Any]) -> List[Any]:
        """Failure extraction from results for retry."""
        pass


class L2QFeatureEngineering(FeatureEngineering):
    """Feature engineering for Level 2 Quote data."""
    
    def feature_computation(self, data: pl.LazyFrame) -> pl.LazyFrame:
        """L2Q feature computation with bar aggregation."""
        # Add bar_id using TimeBarFeatureEngineering
        df = TimeBarFeatureEngineering.bar_time_addition(data, 'TimestampNanoseconds', self.bar_duration_ms)
        
        # Group by bar and compute L2Q features
        return df.group_by(['bar_id', 'Ticker', 'ISOExchangeCode']).agg([
            # Timestamp features
            pl.col('TimestampNanoseconds').min().alias('bar_start_ns'),
            pl.col('TimestampNanoseconds').max().alias('bar_end_ns'),
            pl.col('TimestampNanoseconds').count().alias('quote_count'),
            
            # Bid features
            pl.col('BidPrice1').mean().alias('bid_price_mean'),
            pl.col('BidPrice1').first().alias('bid_price_open'),
            pl.col('BidPrice1').last().alias('bid_price_close'),
            pl.col('BidQuantity1').mean().alias('bid_quantity_mean'),
            
            # Ask features  
            pl.col('AskPrice1').mean().alias('ask_price_mean'),
            pl.col('AskPrice1').first().alias('ask_price_open'),
            pl.col('AskPrice1').last().alias('ask_price_close'),
            pl.col('AskQuantity1').mean().alias('ask_quantity_mean'),
            
            # Spread features
            (pl.col('AskPrice1') - pl.col('BidPrice1')).mean().alias('spread_mean'),
            (pl.col('BidQuantity1') - pl.col('AskQuantity1')).mean().alias('quantity_imbalance_mean')
        ])
    
    def failure_extraction(self, results: List[Any]) -> List[Any]:
        """Failed L2Q item extraction."""
        return [r for r in results if r.get('message') != 'success']


class TradeFeatureEngineering(FeatureEngineering):
    """Feature engineering for Trade data."""
    
    def feature_computation(self, data: pl.LazyFrame) -> pl.LazyFrame:
        """Trade feature computation with bar aggregation."""
        # Add bar_id and side_sign
        df = TimeBarFeatureEngineering.bar_time_addition(data, 'TradeTimestampNanoseconds', self.bar_duration_ms)
        df = df.with_columns([
            pl.when(pl.col('AggressorSide') == 1).then(1)
              .when(pl.col('AggressorSide') == 2).then(-1)
              .otherwise(0).alias('side_sign')
        ])
        
        # Group by bar and compute trade features
        return df.group_by(['bar_id', 'ExchangeTicker', 'Ticker', 'ISOExchangeCode', 'MIC', 'OPOL', 'ExecutionVenue']).agg([
            # Timestamp features
            pl.col('TradeTimestampNanoseconds').min().alias('bar_start_ns'),
            pl.col('TradeTimestampNanoseconds').max().alias('bar_end_ns'),
            pl.col('TradeTimestampNanoseconds').count().alias('trade_count'),
            
            # Price features (OHLC)
            pl.col('Price').first().alias('price_open'),
            pl.col('Price').max().alias('price_high'),
            pl.col('Price').min().alias('price_low'),
            pl.col('Price').last().alias('price_close'),
            pl.col('Price').mean().alias('price_mean'),
            
            # Volume features
            pl.col('Size').sum().alias('volume'),
            pl.col('Size').mean().alias('avg_trade_size'),
            pl.col('Size').max().alias('max_trade_size'),
            
            # VWAP
            (pl.col('Price') * pl.col('Size')).sum().alias('price_volume_sum'),
            ((pl.col('Price') * pl.col('Size')).sum() / pl.col('Size').sum()).alias('vwap'),
            
            # Imbalance features
            (pl.col('Size') * pl.col('Price') * pl.col('side_sign')).sum().alias('volume_imbalance'),
            pl.col('side_sign').sum().alias('trade_imbalance'),
            (pl.col('Size') * pl.col('Price')).filter(pl.col('AggressorSide') == 0).sum().alias('unknown_volume'),
            pl.col('Size').filter(pl.col('AggressorSide') == 0).count().alias('unknown_count'),
            (pl.col('Size') * pl.col('Price')).sum().alias('total_volume'),
            pl.len().alias('total_trades')
        ]).with_columns([
            (pl.col('volume_imbalance') / pl.col('total_volume')).alias('volume_imbalance_ratio'),
            (pl.col('trade_imbalance') / pl.col('total_trades')).alias('trade_imbalance_ratio'),
            (pl.col('unknown_volume') / pl.col('total_volume')).alias('unknown_volume_ratio'),
            (pl.col('unknown_count') / pl.col('total_trades')).alias('unknown_count_ratio')
        ])
    
    def failure_extraction(self, results: List[Any]) -> List[Any]:
        """Failed trade item extraction."""
        return [r for r in results if r.get('message') != 'success']