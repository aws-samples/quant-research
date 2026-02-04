"""Feature engineering with bar aggregation."""
from abc import ABC, abstractmethod
from collections import defaultdict
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
    
    def discover_files(self, data_access, input_path: str, file_sort_order: str) -> List[List[tuple[str, int]]]:
        """Discover normalized parquet files grouped by date/region/exchange.
        
        Args:
            data_access: Data access instance
            input_path: S3 path to normalized data
            file_sort_order: Sort order ('asc' or 'desc')
            
        Returns:
            List of file groups, where each group contains [(file_path, size), ...] for same day/region/exchange
        """
        # List all files in normalized directory
        files = data_access.list_files(input_path)
        
        # Filter for .parquet files only
        parquet_files = [(file_path, size) for file_path, size in files if file_path.endswith('.parquet')]
        
        # Group files by (yyyy, mm, dd, region, exchange)
        groups = defaultdict(list)
        
        for file_path, size in parquet_files:
            try:
                # Parse: "2024/01/02/trades/AMERICAS/trades-ARCX-20240102.parquet"
                parts = file_path.split('/')
                if len(parts) < 5:
                    continue
                    
                yyyy, mm, dd, data_type, region = parts[-5:]
                filename = parts[-1]
                
                # Extract exchange from filename
                if data_type == 'trades':
                    # trades-ARCX-20240102.parquet → ARCX
                    exchange = filename.split('-')[1] if '-' in filename else 'unknown'
                elif data_type == 'level2q':
                    # ARCX-20240102.parquet → ARCX
                    exchange = filename.split('-')[0] if '-' in filename else 'unknown'
                else:
                    continue  # Skip non-trades/level2q files
                
                key = (yyyy, mm, dd, region, exchange)
                groups[key].append((file_path, size))
                
            except (IndexError, ValueError):
                continue  # Skip malformed paths
        
        # Convert to list and sort groups
        group_list = list(groups.values())
        
        # Sort each group internally by file path
        for group in group_list:
            if file_sort_order == "desc":
                group.sort(key=lambda x: x[0], reverse=True)
            else:
                group.sort(key=lambda x: x[0])
        
        # Sort groups by first file in each group
        if file_sort_order == "desc":
            group_list.sort(key=lambda group: group[0][0] if group else '', reverse=True)
        else:
            group_list.sort(key=lambda group: group[0][0] if group else '')
            
        return group_list
    
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


class OrderFlowFeatureEngineering(FeatureEngineering):
    """Unified feature engineering for both Trade and L2Q data."""
    
    def feature_computation(self, data: pl.LazyFrame, data_type: str = None) -> pl.LazyFrame:
        """Route to appropriate feature computation based on data type."""
        # Auto-detect data type if not provided
        if data_type is None:
            columns = data.collect_schema().names()
            if 'BidPrice1' in columns and 'AskPrice1' in columns:
                data_type = 'level2q'
            elif 'TradeTimestampNanoseconds' in columns:
                data_type = 'trades'
            else:
                raise ValueError(f"Cannot determine data type from columns: {columns[:10]}...")
        
        if data_type == 'level2q':
            return self._l2q_feature_computation(data)
        elif data_type == 'trades':
            return self._trade_feature_computation(data)
        else:
            raise ValueError(f"Unsupported data type: {data_type}")
    
    def get_failed_items(self, results: List[Any]) -> List[List[tuple[str, int]]]:
        """Extract failed items for retry, maintaining group structure."""
        # Group failed items by their original group key
        failed_groups = defaultdict(list)
        
        for result in results:
            if result['message'] != 'success' and result['input_path'] != 'group_error':
                file_path = result['input_path']
                file_size = int(result.get('size_gb', 1.0) * (1024 ** 3))  # Convert back to bytes
                
                # Extract grouping key from file path
                try:
                    parts = file_path.split('/')
                    if len(parts) >= 5:
                        yyyy, mm, dd, data_type, region = parts[-5:]
                        filename = parts[-1]
                        
                        if data_type == 'trades':
                            exchange = filename.split('-')[1] if '-' in filename else 'unknown'
                        elif data_type == 'level2q':
                            exchange = filename.split('-')[0] if '-' in filename else 'unknown'
                        else:
                            continue
                        
                        key = (yyyy, mm, dd, region, exchange)
                        failed_groups[key].append((file_path, file_size))
                except (IndexError, ValueError):
                    # If we can't parse the path, create a single-item group
                    failed_groups[file_path].append((file_path, file_size))
        
        return list(failed_groups.values())
    
    def failure_extraction(self, results: List[Any]) -> List[Any]:
        """Unified failure extraction."""
        return [r for r in results if r.get('message') != 'success']


class UnifiedFeatureEngineering(FeatureEngineering):
    """Unified feature engineering for both Trade and L2Q data."""
    
    def __init__(self, bar_duration_ms: int = 1000, max_retries: int = 3):
        super().__init__(bar_duration_ms, max_retries)
        self.l2q_eng = L2QFeatureEngineering(bar_duration_ms, max_retries)
        self.trade_eng = TradeFeatureEngineering(bar_duration_ms, max_retries)
    
    def feature_computation(self, data: pl.LazyFrame) -> pl.LazyFrame:
        """Route to appropriate feature computation based on data type."""
        # Check columns to determine data type
        columns = data.collect_schema().names()
        
        if 'BidPrice1' in columns and 'AskPrice1' in columns:
            return self.l2q_eng.feature_computation(data)
        elif 'TradeTimestampNanoseconds' in columns:
            return self.trade_eng.feature_computation(data)
        else:
            raise ValueError(f"Unknown data type - columns: {columns[:10]}...")
    
    def failure_extraction(self, results: List[Any]) -> List[Any]:
        """Unified failure extraction."""
        return [r for r in results if r.get('message') != 'success']


    def _l2q_feature_computation(self, data: pl.LazyFrame) -> pl.LazyFrame:
        """L2Q feature computation pipeline."""
        # Add bar_id and bar_id_dt
        df = TimeBarFeatureEngineering.bar_time_addition(data, 'TimestampNanoseconds', self.bar_duration_ms)
        
        # Sort by grouping keys and timestamp to ensure proper shift() order
        df = df.sort(['TradeDate', 'Ticker', 'ISOExchangeCode', 'MIC', 'ExchangeTicker', 'TimestampNanoseconds'])
        
        # Build feature pipeline
        pipeline = {
            'section1': self._section1_bar_metadata(df),
            'section2': self._section2_quote_activity(df),
            'section3': self._section3_spread_features(df),
            'section4': self._section4_quantity_features(df),
            'section5': self._section5_volume_features(df),
            'section6': self._section6_volatility_features(df),
            'section7': self._section7_trend_features(df),
            'section8': self._section8_trend_vol_features(df)
        }
        
        # Flatten all features
        features = []
        for section, exprs in pipeline.items():
            features.extend(exprs)
        
        # Group by bar and compute all features
        return df.group_by(['bar_id', 'TradeDate', 'Ticker', 'ISOExchangeCode', 'MIC', 'ExchangeTicker']).agg(features)
    
    def _trade_feature_computation(self, data: pl.LazyFrame) -> pl.LazyFrame:
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
    
    def _slope(self, y_col: str, x_col: str = 'TimestampNanoseconds') -> pl.Expr:
        """Calculate slope using linear regression."""
        x = pl.col(x_col) / 1e9
        y = pl.col(y_col)
        
        n = pl.len()
        sum_x = x.sum()
        sum_y = y.sum()
        sum_xy = (x * y).sum()
        sum_x2 = (x * x).sum()
        
        return (n * sum_xy - sum_x * sum_y) / (n * sum_x2 - sum_x * sum_x)
    
    def _mse_trend(self, y_col: str, x_col: str = 'TimestampNanoseconds') -> pl.Expr:
        """Calculate MSE from linear trend."""
        x = pl.col(x_col) / 1e9
        y = pl.col(y_col)
        
        n = pl.len()
        sum_x = x.sum()
        sum_y = y.sum()
        sum_xy = (x * y).sum()
        sum_x2 = (x * x).sum()
        
        slope = (n * sum_xy - sum_x * sum_y) / (n * sum_x2 - sum_x * sum_x)
        intercept = (sum_y - slope * sum_x) / n
        
        predicted = slope * x + intercept
        return ((y - predicted) ** 2).mean()
    
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
            
            # Bid update counts (levels 1-10)
            *[((pl.col(f'BidPrice{i}') != pl.col(f'BidPrice{i}').shift(1)).cast(pl.Int32) + 
               (pl.col(f'BidQuantity{i}') != pl.col(f'BidQuantity{i}').shift(1)).cast(pl.Int32) + 
               (pl.col(f'BidNumOrders{i}') != pl.col(f'BidNumOrders{i}').shift(1)).cast(pl.Int32)).sum().alias(f'bid_update_count_l{i}') 
              for i in range(1, 11)],
            
            # Ask update counts (levels 1-10)
            *[((pl.col(f'AskPrice{i}') != pl.col(f'AskPrice{i}').shift(1)).cast(pl.Int32) + 
               (pl.col(f'AskQuantity{i}') != pl.col(f'AskQuantity{i}').shift(1)).cast(pl.Int32) + 
               (pl.col(f'AskNumOrders{i}') != pl.col(f'AskNumOrders{i}').shift(1)).cast(pl.Int32)).sum().alias(f'ask_update_count_l{i}') 
              for i in range(1, 11)]
        ]
    
    def _section3_spread_features(self, df: pl.LazyFrame) -> List[pl.Expr]:
        """Section 3: Spread Features."""
        features = []
        for level in range(1, 11):
            features.extend([
                ((pl.col(f'BidPrice{level}') * pl.col(f'BidQuantity{level}') + pl.col(f'AskPrice{level}') * pl.col(f'AskQuantity{level}')) / 
                 (pl.col(f'BidQuantity{level}') + pl.col(f'AskQuantity{level}'))).median().alias(f'volume_weighted_mid_l{level}'),
                ((pl.col(f'AskPrice{level}') - pl.col(f'BidPrice{level}')) / 
                 ((pl.col(f'BidPrice{level}') + pl.col(f'AskPrice{level}')) / 2)).median().alias(f'spread_ratio_l{level}')
            ])
        return features
    
    def _section4_quantity_features(self, df: pl.LazyFrame) -> List[pl.Expr]:
        """Section 4: Quantity Features."""
        return [((pl.col(f'BidQuantity{i}') - pl.col(f'AskQuantity{i}')) / 
                 (pl.col(f'BidQuantity{i}') + pl.col(f'AskQuantity{i}'))).median().alias(f'quantity_imbalance_l{i}') 
                for i in range(1, 11)]
    
    def _section5_volume_features(self, df: pl.LazyFrame) -> List[pl.Expr]:
        """Section 5: Volume Features."""
        return [((pl.col(f'BidPrice{i}') * pl.col(f'BidQuantity{i}') - pl.col(f'AskPrice{i}') * pl.col(f'AskQuantity{i}')) / 
                 (pl.col(f'BidPrice{i}') * pl.col(f'BidQuantity{i}') + pl.col(f'AskPrice{i}') * pl.col(f'AskQuantity{i}'))).median().alias(f'volume_imbalance_l{i}') 
                for i in range(1, 11)]
    
    def _section6_volatility_features(self, df: pl.LazyFrame) -> List[pl.Expr]:
        """Section 6: Volatility Features."""
        features = []
        for level in range(1, 11):
            features.extend([
                (pl.col(f'BidPrice{level}').std() / pl.col(f'BidPrice{level}').median()).alias(f'bid_price_volatility_l{level}'),
                (pl.col(f'AskPrice{level}').std() / pl.col(f'AskPrice{level}').median()).alias(f'ask_price_volatility_l{level}'),
                (((pl.col(f'BidPrice{level}') + pl.col(f'AskPrice{level}')) / 2).std() / 
                 ((pl.col(f'BidPrice{level}') + pl.col(f'AskPrice{level}')) / 2).median()).alias(f'mid_price_volatility_l{level}'),
                (pl.col(f'BidQuantity{level}').std() / pl.col(f'BidQuantity{level}').median()).alias(f'bid_quantity_volatility_l{level}'),
                (pl.col(f'AskQuantity{level}').std() / pl.col(f'AskQuantity{level}').median()).alias(f'ask_quantity_volatility_l{level}'),
                ((pl.col(f'BidPrice{level}') * pl.col(f'BidQuantity{level}')).std() / 
                 (pl.col(f'BidPrice{level}') * pl.col(f'BidQuantity{level}')).median()).alias(f'bid_volume_volatility_l{level}'),
                ((pl.col(f'AskPrice{level}') * pl.col(f'AskQuantity{level}')).std() / 
                 (pl.col(f'AskPrice{level}') * pl.col(f'AskQuantity{level}')).median()).alias(f'ask_volume_volatility_l{level}')
            ])
        return features
    
    def _section7_trend_features(self, df: pl.LazyFrame) -> List[pl.Expr]:
        """Section 7: Trend Features."""
        features = []
        for level in range(1, 11):
            features.extend([
                self._slope(f'BidPrice{level}').alias(f'bid_price_trend_l{level}'),
                self._slope(f'AskPrice{level}').alias(f'ask_price_trend_l{level}'),
                self._slope_mid_price(f'BidPrice{level}', f'AskPrice{level}').alias(f'mid_price_trend_l{level}'),
                self._slope(f'BidQuantity{level}').alias(f'bid_quantity_trend_l{level}'),
                self._slope(f'AskQuantity{level}').alias(f'ask_quantity_trend_l{level}'),
                self._slope_product(f'BidPrice{level}', f'BidQuantity{level}').alias(f'bid_volume_trend_l{level}'),
                self._slope_product(f'AskPrice{level}', f'AskQuantity{level}').alias(f'ask_volume_trend_l{level}')
            ])
        return features
    
    def _section8_trend_vol_features(self, df: pl.LazyFrame) -> List[pl.Expr]:
        """Section 8: Trend Vol Features."""
        features = []
        for level in range(1, 11):
            features.extend([
                (self._mse_trend(f'BidPrice{level}') / pl.col(f'BidPrice{level}').median()).alias(f'bid_price_trend_vol_l{level}'),
                (self._mse_trend(f'AskPrice{level}') / pl.col(f'AskPrice{level}').median()).alias(f'ask_price_trend_vol_l{level}'),
                (self._mse_trend_mid_price(f'BidPrice{level}', f'AskPrice{level}') / 
                 ((pl.col(f'BidPrice{level}') + pl.col(f'AskPrice{level}')) / 2).median()).alias(f'mid_price_trend_vol_l{level}'),
                (self._mse_trend(f'BidQuantity{level}') / pl.col(f'BidQuantity{level}').median()).alias(f'bid_quantity_trend_vol_l{level}'),
                (self._mse_trend(f'AskQuantity{level}') / pl.col(f'AskQuantity{level}').median()).alias(f'ask_quantity_trend_vol_l{level}'),
                (self._mse_trend_product(f'BidPrice{level}', f'BidQuantity{level}') / 
                 (pl.col(f'BidPrice{level}') * pl.col(f'BidQuantity{level}')).median()).alias(f'bid_volume_trend_vol_l{level}'),
                (self._mse_trend_product(f'AskPrice{level}', f'AskQuantity{level}') / 
                 (pl.col(f'AskPrice{level}') * pl.col(f'AskQuantity{level}')).median()).alias(f'ask_volume_trend_vol_l{level}')
            ])
        return features
    
    def _slope_mid_price(self, bid_col: str, ask_col: str, x_col: str = 'TimestampNanoseconds') -> pl.Expr:
        """Calculate slope for mid price."""
        x = pl.col(x_col) / 1e9
        y = (pl.col(bid_col) + pl.col(ask_col)) / 2
        
        n = pl.len()
        sum_x = x.sum()
        sum_y = y.sum()
        sum_xy = (x * y).sum()
        sum_x2 = (x * x).sum()
        
        return (n * sum_xy - sum_x * sum_y) / (n * sum_x2 - sum_x * sum_x)
    
    def _slope_product(self, col1: str, col2: str, x_col: str = 'TimestampNanoseconds') -> pl.Expr:
        """Calculate slope for product of two columns."""
        x = pl.col(x_col) / 1e9
        y = pl.col(col1) * pl.col(col2)
        
        n = pl.len()
        sum_x = x.sum()
        sum_y = y.sum()
        sum_xy = (x * y).sum()
        sum_x2 = (x * x).sum()
        
        return (n * sum_xy - sum_x * sum_y) / (n * sum_x2 - sum_x * sum_x)
    
    def _mse_trend_mid_price(self, bid_col: str, ask_col: str, x_col: str = 'TimestampNanoseconds') -> pl.Expr:
        """Calculate MSE from linear trend for mid price."""
        x = pl.col(x_col) / 1e9
        y = (pl.col(bid_col) + pl.col(ask_col)) / 2
        
        n = pl.len()
        sum_x = x.sum()
        sum_y = y.sum()
        sum_xy = (x * y).sum()
        sum_x2 = (x * x).sum()
        
        slope = (n * sum_xy - sum_x * sum_y) / (n * sum_x2 - sum_x * sum_x)
        intercept = (sum_y - slope * sum_x) / n
        
        predicted = slope * x + intercept
        return ((y - predicted) ** 2).mean()
    
    def _mse_trend_product(self, col1: str, col2: str, x_col: str = 'TimestampNanoseconds') -> pl.Expr:
        """Calculate MSE from linear trend for product of two columns."""
        x = pl.col(x_col) / 1e9
        y = pl.col(col1) * pl.col(col2)
        
        n = pl.len()
        sum_x = x.sum()
        sum_y = y.sum()
        sum_xy = (x * y).sum()
        sum_x2 = (x * x).sum()
        
        slope = (n * sum_xy - sum_x * sum_y) / (n * sum_x2 - sum_x * sum_x)
        intercept = (sum_y - slope * sum_x) / n
        
        predicted = slope * x + intercept
        return ((y - predicted) ** 2).mean()


# Legacy classes for backward compatibility
class L2QFeatureEngineering(OrderFlowFeatureEngineering):
    """Legacy L2Q feature engineering - use OrderFlowFeatureEngineering instead."""
    pass

class TradeFeatureEngineering(OrderFlowFeatureEngineering):
    """Legacy Trade feature engineering - use OrderFlowFeatureEngineering instead."""
    pass