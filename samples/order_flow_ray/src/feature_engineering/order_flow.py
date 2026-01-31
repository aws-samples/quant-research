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
    
    def _section1_bar_metadata(self, df: pl.LazyFrame) -> List[pl.Expr]:
        """Section 1: Bar Metadata Features."""
        return [
            pl.col('bar_id_dt').first().alias('bar_id_dt'),
            pl.col('bar_duration_ms').first().alias('bar_duration_ms'),
            pl.col('MarketState').mode().first().alias('market_state_mode')
        ]
    
    def _section2_quote_activity(self, df: pl.LazyFrame) -> List[pl.Expr]:
        """Section 2: Quote Activity Features."""
        return [
            pl.col('TimestampNanoseconds').min().alias('bar_start_dt'),
            pl.col('TimestampNanoseconds').max().alias('bar_end_dt'),
            pl.col('TimestampNanoseconds').count().alias('quote_count'),
            
            # Bid update counts (levels 1-10): price_change + quantity_change + orders_change
            ((pl.col('BidPrice1').diff().abs() > 0).cast(pl.Int32) + 
             (pl.col('BidQuantity1').diff().abs() > 0).cast(pl.Int32) + 
             (pl.col('BidNumOrders1').diff().abs() > 0).cast(pl.Int32)).sum().alias('bid_update_count_l1'),
            ((pl.col('BidPrice2').diff().abs() > 0).cast(pl.Int32) + 
             (pl.col('BidQuantity2').diff().abs() > 0).cast(pl.Int32) + 
             (pl.col('BidNumOrders2').diff().abs() > 0).cast(pl.Int32)).sum().alias('bid_update_count_l2'),
            ((pl.col('BidPrice3').diff().abs() > 0).cast(pl.Int32) + 
             (pl.col('BidQuantity3').diff().abs() > 0).cast(pl.Int32) + 
             (pl.col('BidNumOrders3').diff().abs() > 0).cast(pl.Int32)).sum().alias('bid_update_count_l3'),
            ((pl.col('BidPrice4').diff().abs() > 0).cast(pl.Int32) + 
             (pl.col('BidQuantity4').diff().abs() > 0).cast(pl.Int32) + 
             (pl.col('BidNumOrders4').diff().abs() > 0).cast(pl.Int32)).sum().alias('bid_update_count_l4'),
            ((pl.col('BidPrice5').diff().abs() > 0).cast(pl.Int32) + 
             (pl.col('BidQuantity5').diff().abs() > 0).cast(pl.Int32) + 
             (pl.col('BidNumOrders5').diff().abs() > 0).cast(pl.Int32)).sum().alias('bid_update_count_l5'),
            ((pl.col('BidPrice6').diff().abs() > 0).cast(pl.Int32) + 
             (pl.col('BidQuantity6').diff().abs() > 0).cast(pl.Int32) + 
             (pl.col('BidNumOrders6').diff().abs() > 0).cast(pl.Int32)).sum().alias('bid_update_count_l6'),
            ((pl.col('BidPrice7').diff().abs() > 0).cast(pl.Int32) + 
             (pl.col('BidQuantity7').diff().abs() > 0).cast(pl.Int32) + 
             (pl.col('BidNumOrders7').diff().abs() > 0).cast(pl.Int32)).sum().alias('bid_update_count_l7'),
            ((pl.col('BidPrice8').diff().abs() > 0).cast(pl.Int32) + 
             (pl.col('BidQuantity8').diff().abs() > 0).cast(pl.Int32) + 
             (pl.col('BidNumOrders8').diff().abs() > 0).cast(pl.Int32)).sum().alias('bid_update_count_l8'),
            ((pl.col('BidPrice9').diff().abs() > 0).cast(pl.Int32) + 
             (pl.col('BidQuantity9').diff().abs() > 0).cast(pl.Int32) + 
             (pl.col('BidNumOrders9').diff().abs() > 0).cast(pl.Int32)).sum().alias('bid_update_count_l9'),
            ((pl.col('BidPrice10').diff().abs() > 0).cast(pl.Int32) + 
             (pl.col('BidQuantity10').diff().abs() > 0).cast(pl.Int32) + 
             (pl.col('BidNumOrders10').diff().abs() > 0).cast(pl.Int32)).sum().alias('bid_update_count_l10'),
            
            # Ask update counts (levels 1-10): price_change + quantity_change + orders_change
            ((pl.col('AskPrice1').diff().abs() > 0).cast(pl.Int32) + 
             (pl.col('AskQuantity1').diff().abs() > 0).cast(pl.Int32) + 
             (pl.col('AskNumOrders1').diff().abs() > 0).cast(pl.Int32)).sum().alias('ask_update_count_l1'),
            ((pl.col('AskPrice2').diff().abs() > 0).cast(pl.Int32) + 
             (pl.col('AskQuantity2').diff().abs() > 0).cast(pl.Int32) + 
             (pl.col('AskNumOrders2').diff().abs() > 0).cast(pl.Int32)).sum().alias('ask_update_count_l2'),
            ((pl.col('AskPrice3').diff().abs() > 0).cast(pl.Int32) + 
             (pl.col('AskQuantity3').diff().abs() > 0).cast(pl.Int32) + 
             (pl.col('AskNumOrders3').diff().abs() > 0).cast(pl.Int32)).sum().alias('ask_update_count_l3'),
            ((pl.col('AskPrice4').diff().abs() > 0).cast(pl.Int32) + 
             (pl.col('AskQuantity4').diff().abs() > 0).cast(pl.Int32) + 
             (pl.col('AskNumOrders4').diff().abs() > 0).cast(pl.Int32)).sum().alias('ask_update_count_l4'),
            ((pl.col('AskPrice5').diff().abs() > 0).cast(pl.Int32) + 
             (pl.col('AskQuantity5').diff().abs() > 0).cast(pl.Int32) + 
             (pl.col('AskNumOrders5').diff().abs() > 0).cast(pl.Int32)).sum().alias('ask_update_count_l5'),
            ((pl.col('AskPrice6').diff().abs() > 0).cast(pl.Int32) + 
             (pl.col('AskQuantity6').diff().abs() > 0).cast(pl.Int32) + 
             (pl.col('AskNumOrders6').diff().abs() > 0).cast(pl.Int32)).sum().alias('ask_update_count_l6'),
            ((pl.col('AskPrice7').diff().abs() > 0).cast(pl.Int32) + 
             (pl.col('AskQuantity7').diff().abs() > 0).cast(pl.Int32) + 
             (pl.col('AskNumOrders7').diff().abs() > 0).cast(pl.Int32)).sum().alias('ask_update_count_l7'),
            ((pl.col('AskPrice8').diff().abs() > 0).cast(pl.Int32) + 
             (pl.col('AskQuantity8').diff().abs() > 0).cast(pl.Int32) + 
             (pl.col('AskNumOrders8').diff().abs() > 0).cast(pl.Int32)).sum().alias('ask_update_count_l8'),
            ((pl.col('AskPrice9').diff().abs() > 0).cast(pl.Int32) + 
             (pl.col('AskQuantity9').diff().abs() > 0).cast(pl.Int32) + 
             (pl.col('AskNumOrders9').diff().abs() > 0).cast(pl.Int32)).sum().alias('ask_update_count_l9'),
            ((pl.col('AskPrice10').diff().abs() > 0).cast(pl.Int32) + 
             (pl.col('AskQuantity10').diff().abs() > 0).cast(pl.Int32) + 
             (pl.col('AskNumOrders10').diff().abs() > 0).cast(pl.Int32)).sum().alias('ask_update_count_l10')
        ]
    
    def feature_computation(self, data: pl.LazyFrame) -> pl.LazyFrame:
        """L2Q feature computation pipeline."""
        # Add bar_id and bar_id_dt
        df = TimeBarFeatureEngineering.bar_time_addition(data, 'TimestampNanoseconds', self.bar_duration_ms)
        
        # Build feature pipeline
        pipeline = {
            'section1': self._section1_bar_metadata(df),
            'section2': self._section2_quote_activity(df)
        }
        
        # Flatten all features
        features = []
        for section, exprs in pipeline.items():
            features.extend(exprs)
        
        # Group by bar and compute all features
        return df.group_by(['bar_id', 'TradeDate', 'Ticker', 'ISOExchangeCode', 'MIC', 'ExchangeTicker']).agg(features)
    
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
        return df.group_by(['bar_id', 'TradeDate', 'ExchangeTicker', 'Ticker', 'ISOExchangeCode', 'MIC', 'OPOL', 'ExecutionVenue']).agg([
            # Bar datetime (preserve from bar_id_dt)
            pl.col('bar_id_dt').first().alias('bar_id_dt'),
            # Bar duration (preserve from bar_duration_ms)
            pl.col('bar_duration_ms').first().alias('bar_duration_ms'),
            # Timestamp features
            pl.col('TradeTimestampNanoseconds').min().alias('bar_start_ns'),
            pl.col('TradeTimestampNanoseconds').max().alias('bar_end_ns'),
            pl.col('TradeTimestampNanoseconds').count().alias('trade_count'),
            pl.col('TradeTimestamp').min().alias('trade_timestamp_min'),
            pl.col('TradeTimestamp').max().alias('trade_timestamp_max'),
            pl.col('PublicationTimestamp').min().alias('publication_timestamp_min'),
            pl.col('PublicationTimestamp').max().alias('publication_timestamp_max'),
            pl.col('LocalTradeTimestamp').min().alias('local_trade_timestamp_min'),
            pl.col('LocalTradeTimestamp').max().alias('local_trade_timestamp_max'),
            pl.col('LocalPublicationTimestamp').min().alias('local_publication_timestamp_min'),
            pl.col('LocalPublicationTimestamp').max().alias('local_publication_timestamp_max'),
            
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
            (pl.col('Size') * pl.col('Price')).filter(pl.col('AggressorSide') == 0).sum().alias('unassigned_volume'),
            pl.col('Size').filter(pl.col('AggressorSide') == 0).count().alias('unassigned_count'),
            (pl.col('Size') * pl.col('Price')).sum().alias('total_volume'),
            pl.len().alias('total_trades')
        ]).with_columns([
            (pl.col('volume_imbalance') / pl.col('total_volume')).alias('volume_imbalance_ratio'),
            (pl.col('trade_imbalance') / pl.col('total_trades')).alias('trade_imbalance_ratio'),
            (pl.col('unassigned_volume') / pl.col('total_volume')).alias('unassigned_volume_ratio'),
            (pl.col('unassigned_count') / pl.col('total_trades')).alias('unassigned_count_ratio')
        ])
    
    def failure_extraction(self, results: List[Any]) -> List[Any]:
        """Failed trade item extraction."""
        return [r for r in results if r.get('message') != 'success']