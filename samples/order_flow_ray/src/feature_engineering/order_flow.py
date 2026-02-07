"""Feature engineering with bar aggregation."""
from abc import ABC, abstractmethod
from typing import Any, List
import polars as pl
from .base import TimeBarFeatureEngineering


class FeatureEngineering(ABC):
    """Base class for feature engineering with configurable bar aggregation."""
    
    def __init__(self, bar_duration_ms: int = 1000, max_retries: int = 3, discovery_mode: str = 'asynch'):
        """Feature engineering initialization.
        
        Args:
            bar_duration_ms: Bar duration in milliseconds
            max_retries: Maximum retry attempts
            discovery_mode: 'synch' or 'asynch' for file discovery
        """
        self.bar_duration_ms = bar_duration_ms
        self.max_retries = max_retries
        self.discovery_mode = discovery_mode
    
    @abstractmethod
    def feature_computation(self, data: pl.LazyFrame) -> pl.LazyFrame:
        """Feature computation from normalized data.
        
        Args:
            data: Normalized data
            
        Returns:
            Data with computed features
        """
        pass


class OrderFlowFeatureEngineering(FeatureEngineering):
    """Unified feature engineering for both L2Q and Trade data."""
    
    def feature_computation(self, data: pl.LazyFrame, data_type: str) -> pl.LazyFrame:
        """Unified feature computation for both data types.
        
        Args:
            data: Input data
            data_type: 'level2q' or 'trades'
            
        Returns:
            Features dataframe
        """
        if data_type == 'level2q':
            l2q_eng = L2QFeatureEngineering(self.bar_duration_ms, self.max_retries)
            return l2q_eng.feature_computation(data)
        else:
            trade_eng = TradeFeatureEngineering(self.bar_duration_ms, self.max_retries)
            return trade_eng.feature_computation(data)
    
    def discover_files(self, data_access, normalized_data_path: str, sort_order: str) -> list[tuple[str, int]]:
        """Discover normalized files to process.
        
        Args:
            data_access: Data access instance
            normalized_data_path: Base path to normalized data
            sort_order: Sort order - 'asc' or 'desc'
            
        Returns:
            List of (file_path, file_size) tuples sorted by size
        """
        if self.discovery_mode == 'asynch':
            return self.discover_files_asynch(data_access, normalized_data_path, sort_order)
        
        print(f"Discovering files in: {normalized_data_path}")
        files = data_access.list_files(normalized_data_path)
        files.sort(key=lambda x: x[1], reverse=(sort_order == 'desc'))
        return files
    
    def discover_files_asynch(self, data_access, normalized_data_path: str, sort_order: str, parallel_discovery_threshold: int = 100) -> list[tuple[str, int]]:
        """Discover normalized files using parallel listing.
        
        Args:
            data_access: Data access instance
            normalized_data_path: Base path to normalized data
            sort_order: Sort order - 'asc' or 'desc'
            parallel_discovery_threshold: Switch to parallel when prefix count exceeds this
            
        Returns:
            List of (file_path, file_size) tuples sorted by size
        """
        print(f"Discovering files in: {normalized_data_path}")
        files = data_access.list_files_asynch(normalized_data_path, parallel_discovery_threshold)
        files.sort(key=lambda x: x[1], reverse=(sort_order == 'desc'))
        return files
    
    def get_failed_items(self, results: list) -> list[tuple[str, int]]:
        """Extract failed files from feature engineering results.
        
        Args:
            results: List of feature engineering results
            
        Returns:
            List of (file_path, file_size) tuples that failed
        """
        failed = [r for r in results if r['message'] != 'success']
        return [(r['input_path'], int(r.get('size_gb', 1.0) * (1024**3))) for r in failed]
    
    def group_files_for_processing(self, files: list[tuple[str, int]]) -> list[list[tuple[str, int]]]:
        """Group files by exchange for batch processing.
        
        Args:
            files: List of (file_path, file_size) tuples
            
        Returns:
            List of file groups, each group contains trades+level2q files for same exchange
        """
        from collections import defaultdict
        
        groups = defaultdict(list)
        
        for file_path, file_size in files:
            # Extract exchange from filename
            # Expected format: trades-XASE-20240604.parquet or XASE-20240604.parquet
            filename = file_path.split('/')[-1]
            
            if filename.startswith('trades-'):
                exchange = filename.split('-')[1]
            elif '-' in filename:
                exchange = filename.split('-')[0]
            else:
                exchange = filename.split('.')[0]
            
            groups[exchange].append((file_path, file_size))
        
        return list(groups.values())


class L2QFeatureEngineering(FeatureEngineering):
    """Feature engineering for Level 2 Quote data."""
    
    def _slope(self, y_col: str, x_col: str = 'TimestampNanoseconds') -> pl.Expr:
        """Calculate slope using linear regression."""
        # Convert nanoseconds to seconds for numerical stability
        x = pl.col(x_col) / 1e9
        y = pl.col(y_col)
        
        # Calculate slope: (n*Σxy - ΣxΣy) / (n*Σx² - (Σx)²)
        n = pl.len()
        sum_x = x.sum()
        sum_y = y.sum()
        sum_xy = (x * y).sum()
        sum_x2 = (x * x).sum()
        
        return (n * sum_xy - sum_x * sum_y) / (n * sum_x2 - sum_x * sum_x)
    
    def _mse_trend(self, y_col: str, x_col: str = 'TimestampNanoseconds') -> pl.Expr:
        """Calculate MSE from linear trend."""
        # Convert nanoseconds to seconds for numerical stability
        x = pl.col(x_col) / 1e9
        y = pl.col(y_col)
        
        # Calculate linear regression coefficients
        n = pl.len()
        sum_x = x.sum()
        sum_y = y.sum()
        sum_xy = (x * y).sum()
        sum_x2 = (x * x).sum()
        
        slope = (n * sum_xy - sum_x * sum_y) / (n * sum_x2 - sum_x * sum_x)
        intercept = (sum_y - slope * sum_x) / n
        
        # Calculate MSE: mean((y - (slope*x + intercept))²)
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
            
            # Bid update counts (levels 1-10): price_change + quantity_change + orders_change
            ((pl.col('BidPrice1') != pl.col('BidPrice1').shift(1)).cast(pl.Int32) + 
             (pl.col('BidQuantity1') != pl.col('BidQuantity1').shift(1)).cast(pl.Int32) + 
             (pl.col('BidNumOrders1') != pl.col('BidNumOrders1').shift(1)).cast(pl.Int32)).sum().alias('bid_update_count_l1'),
            ((pl.col('BidPrice2') != pl.col('BidPrice2').shift(1)).cast(pl.Int32) + 
             (pl.col('BidQuantity2') != pl.col('BidQuantity2').shift(1)).cast(pl.Int32) + 
             (pl.col('BidNumOrders2') != pl.col('BidNumOrders2').shift(1)).cast(pl.Int32)).sum().alias('bid_update_count_l2'),
            ((pl.col('BidPrice3') != pl.col('BidPrice3').shift(1)).cast(pl.Int32) + 
             (pl.col('BidQuantity3') != pl.col('BidQuantity3').shift(1)).cast(pl.Int32) + 
             (pl.col('BidNumOrders3') != pl.col('BidNumOrders3').shift(1)).cast(pl.Int32)).sum().alias('bid_update_count_l3'),
            ((pl.col('BidPrice4') != pl.col('BidPrice4').shift(1)).cast(pl.Int32) + 
             (pl.col('BidQuantity4') != pl.col('BidQuantity4').shift(1)).cast(pl.Int32) + 
             (pl.col('BidNumOrders4') != pl.col('BidNumOrders4').shift(1)).cast(pl.Int32)).sum().alias('bid_update_count_l4'),
            ((pl.col('BidPrice5') != pl.col('BidPrice5').shift(1)).cast(pl.Int32) + 
             (pl.col('BidQuantity5') != pl.col('BidQuantity5').shift(1)).cast(pl.Int32) + 
             (pl.col('BidNumOrders5') != pl.col('BidNumOrders5').shift(1)).cast(pl.Int32)).sum().alias('bid_update_count_l5'),
            ((pl.col('BidPrice6') != pl.col('BidPrice6').shift(1)).cast(pl.Int32) + 
             (pl.col('BidQuantity6') != pl.col('BidQuantity6').shift(1)).cast(pl.Int32) + 
             (pl.col('BidNumOrders6') != pl.col('BidNumOrders6').shift(1)).cast(pl.Int32)).sum().alias('bid_update_count_l6'),
            ((pl.col('BidPrice7') != pl.col('BidPrice7').shift(1)).cast(pl.Int32) + 
             (pl.col('BidQuantity7') != pl.col('BidQuantity7').shift(1)).cast(pl.Int32) + 
             (pl.col('BidNumOrders7') != pl.col('BidNumOrders7').shift(1)).cast(pl.Int32)).sum().alias('bid_update_count_l7'),
            ((pl.col('BidPrice8') != pl.col('BidPrice8').shift(1)).cast(pl.Int32) + 
             (pl.col('BidQuantity8') != pl.col('BidQuantity8').shift(1)).cast(pl.Int32) + 
             (pl.col('BidNumOrders8') != pl.col('BidNumOrders8').shift(1)).cast(pl.Int32)).sum().alias('bid_update_count_l8'),
            ((pl.col('BidPrice9') != pl.col('BidPrice9').shift(1)).cast(pl.Int32) + 
             (pl.col('BidQuantity9') != pl.col('BidQuantity9').shift(1)).cast(pl.Int32) + 
             (pl.col('BidNumOrders9') != pl.col('BidNumOrders9').shift(1)).cast(pl.Int32)).sum().alias('bid_update_count_l9'),
            ((pl.col('BidPrice10') != pl.col('BidPrice10').shift(1)).cast(pl.Int32) + 
             (pl.col('BidQuantity10') != pl.col('BidQuantity10').shift(1)).cast(pl.Int32) + 
             (pl.col('BidNumOrders10') != pl.col('BidNumOrders10').shift(1)).cast(pl.Int32)).sum().alias('bid_update_count_l10'),
            
            # Ask update counts (levels 1-10): price_change + quantity_change + orders_change
            ((pl.col('AskPrice1') != pl.col('AskPrice1').shift(1)).cast(pl.Int32) + 
             (pl.col('AskQuantity1') != pl.col('AskQuantity1').shift(1)).cast(pl.Int32) + 
             (pl.col('AskNumOrders1') != pl.col('AskNumOrders1').shift(1)).cast(pl.Int32)).sum().alias('ask_update_count_l1'),
            ((pl.col('AskPrice2') != pl.col('AskPrice2').shift(1)).cast(pl.Int32) + 
             (pl.col('AskQuantity2') != pl.col('AskQuantity2').shift(1)).cast(pl.Int32) + 
             (pl.col('AskNumOrders2') != pl.col('AskNumOrders2').shift(1)).cast(pl.Int32)).sum().alias('ask_update_count_l2'),
            ((pl.col('AskPrice3') != pl.col('AskPrice3').shift(1)).cast(pl.Int32) + 
             (pl.col('AskQuantity3') != pl.col('AskQuantity3').shift(1)).cast(pl.Int32) + 
             (pl.col('AskNumOrders3') != pl.col('AskNumOrders3').shift(1)).cast(pl.Int32)).sum().alias('ask_update_count_l3'),
            ((pl.col('AskPrice4') != pl.col('AskPrice4').shift(1)).cast(pl.Int32) + 
             (pl.col('AskQuantity4') != pl.col('AskQuantity4').shift(1)).cast(pl.Int32) + 
             (pl.col('AskNumOrders4') != pl.col('AskNumOrders4').shift(1)).cast(pl.Int32)).sum().alias('ask_update_count_l4'),
            ((pl.col('AskPrice5') != pl.col('AskPrice5').shift(1)).cast(pl.Int32) + 
             (pl.col('AskQuantity5') != pl.col('AskQuantity5').shift(1)).cast(pl.Int32) + 
             (pl.col('AskNumOrders5') != pl.col('AskNumOrders5').shift(1)).cast(pl.Int32)).sum().alias('ask_update_count_l5'),
            ((pl.col('AskPrice6') != pl.col('AskPrice6').shift(1)).cast(pl.Int32) + 
             (pl.col('AskQuantity6') != pl.col('AskQuantity6').shift(1)).cast(pl.Int32) + 
             (pl.col('AskNumOrders6') != pl.col('AskNumOrders6').shift(1)).cast(pl.Int32)).sum().alias('ask_update_count_l6'),
            ((pl.col('AskPrice7') != pl.col('AskPrice7').shift(1)).cast(pl.Int32) + 
             (pl.col('AskQuantity7') != pl.col('AskQuantity7').shift(1)).cast(pl.Int32) + 
             (pl.col('AskNumOrders7') != pl.col('AskNumOrders7').shift(1)).cast(pl.Int32)).sum().alias('ask_update_count_l7'),
            ((pl.col('AskPrice8') != pl.col('AskPrice8').shift(1)).cast(pl.Int32) + 
             (pl.col('AskQuantity8') != pl.col('AskQuantity8').shift(1)).cast(pl.Int32) + 
             (pl.col('AskNumOrders8') != pl.col('AskNumOrders8').shift(1)).cast(pl.Int32)).sum().alias('ask_update_count_l8'),
            ((pl.col('AskPrice9') != pl.col('AskPrice9').shift(1)).cast(pl.Int32) + 
             (pl.col('AskQuantity9') != pl.col('AskQuantity9').shift(1)).cast(pl.Int32) + 
             (pl.col('AskNumOrders9') != pl.col('AskNumOrders9').shift(1)).cast(pl.Int32)).sum().alias('ask_update_count_l9'),
            ((pl.col('AskPrice10') != pl.col('AskPrice10').shift(1)).cast(pl.Int32) + 
             (pl.col('AskQuantity10') != pl.col('AskQuantity10').shift(1)).cast(pl.Int32) + 
             (pl.col('AskNumOrders10') != pl.col('AskNumOrders10').shift(1)).cast(pl.Int32)).sum().alias('ask_update_count_l10')
        ]
    
    def _section3_spread_features(self, df: pl.LazyFrame) -> List[pl.Expr]:
        """Section 3: Spread Features."""
        features = []
        
        # Volume-weighted mid prices and spread ratios for levels 1-10
        for level in range(1, 11):
            bid_price = f'BidPrice{level}'
            ask_price = f'AskPrice{level}'
            bid_qty = f'BidQuantity{level}'
            ask_qty = f'AskQuantity{level}'
            
            # Volume-weighted mid price
            features.append(
                ((pl.col(bid_price) * pl.col(bid_qty) + pl.col(ask_price) * pl.col(ask_qty)) / 
                 (pl.col(bid_qty) + pl.col(ask_qty))).median().alias(f'volume_weighted_mid_l{level}')
            )
            
            # Spread ratio
            features.append(
                ((pl.col(ask_price) - pl.col(bid_price)) / 
                 ((pl.col(bid_price) + pl.col(ask_price)) / 2)).median().alias(f'spread_ratio_l{level}')
            )
        
        return features
    
    def _section4_quantity_features(self, df: pl.LazyFrame) -> List[pl.Expr]:
        """Section 4: Quantity Features."""
        features = []
        
        # Quantity imbalance for levels 1-10
        for level in range(1, 11):
            bid_qty = f'BidQuantity{level}'
            ask_qty = f'AskQuantity{level}'
            
            # Quantity imbalance: (BidQuantity - AskQuantity) / (BidQuantity + AskQuantity)
            features.append(
                ((pl.col(bid_qty) - pl.col(ask_qty)) / 
                 (pl.col(bid_qty) + pl.col(ask_qty))).median().alias(f'quantity_imbalance_l{level}')
            )
        
        return features
    
    def _section5_volume_features(self, df: pl.LazyFrame) -> List[pl.Expr]:
        """Section 5: Volume Features."""
        features = []
        
        # Volume imbalance for levels 1-10
        for level in range(1, 11):
            bid_price = f'BidPrice{level}'
            ask_price = f'AskPrice{level}'
            bid_qty = f'BidQuantity{level}'
            ask_qty = f'AskQuantity{level}'
            
            # Volume imbalance: (BidPrice * BidQuantity - AskPrice * AskQuantity) / (BidPrice * BidQuantity + AskPrice * AskQuantity)
            features.append(
                ((pl.col(bid_price) * pl.col(bid_qty) - pl.col(ask_price) * pl.col(ask_qty)) / 
                 (pl.col(bid_price) * pl.col(bid_qty) + pl.col(ask_price) * pl.col(ask_qty))).median().alias(f'volume_imbalance_l{level}')
            )
        
        return features
    
    def _section6_volatility_features(self, df: pl.LazyFrame) -> List[pl.Expr]:
        """Section 6: Volatility Features."""
        features = []
        
        # Bid/Ask/Mid price volatility for levels 1-10
        for level in range(1, 11):
            bid_price = f'BidPrice{level}'
            ask_price = f'AskPrice{level}'
            bid_qty = f'BidQuantity{level}'
            ask_qty = f'AskQuantity{level}'
            
            # Bid price volatility: stdev(BidPrice) / median(BidPrice)
            features.append(
                (pl.col(bid_price).std() / pl.col(bid_price).median()).alias(f'bid_price_volatility_l{level}')
            )
            
            # Ask price volatility: stdev(AskPrice) / median(AskPrice)
            features.append(
                (pl.col(ask_price).std() / pl.col(ask_price).median()).alias(f'ask_price_volatility_l{level}')
            )
            
            # Mid price volatility: stdev((BidPrice + AskPrice) / 2) / median((BidPrice + AskPrice) / 2)
            mid_price = (pl.col(bid_price) + pl.col(ask_price)) / 2
            features.append(
                (mid_price.std() / mid_price.median()).alias(f'mid_price_volatility_l{level}')
            )
            
            # Bid quantity volatility: stdev(BidQuantity) / median(BidQuantity)
            features.append(
                (pl.col(bid_qty).std() / pl.col(bid_qty).median()).alias(f'bid_quantity_volatility_l{level}')
            )
            
            # Ask quantity volatility: stdev(AskQuantity) / median(AskQuantity)
            features.append(
                (pl.col(ask_qty).std() / pl.col(ask_qty).median()).alias(f'ask_quantity_volatility_l{level}')
            )
            
            # Bid volume volatility: stdev(BidPrice * BidQuantity) / median(BidPrice * BidQuantity)
            bid_volume = pl.col(bid_price) * pl.col(bid_qty)
            features.append(
                (bid_volume.std() / bid_volume.median()).alias(f'bid_volume_volatility_l{level}')
            )
            
            # Ask volume volatility: stdev(AskPrice * AskQuantity) / median(AskPrice * AskQuantity)
            ask_volume = pl.col(ask_price) * pl.col(ask_qty)
            features.append(
                (ask_volume.std() / ask_volume.median()).alias(f'ask_volume_volatility_l{level}')
            )
        
        return features
    
    def _section7_trend_features(self, df: pl.LazyFrame) -> List[pl.Expr]:
        """Section 7: Trend Features."""
        features = []
        
        # Bid/Ask/Mid price trends, quantity trends, volume trends for levels 1-10
        for level in range(1, 11):
            bid_price = f'BidPrice{level}'
            ask_price = f'AskPrice{level}'
            bid_qty = f'BidQuantity{level}'
            ask_qty = f'AskQuantity{level}'
            
            # Bid price trend: slope(BidPrice ORDER BY TimestampNanoseconds)
            features.append(
                self._slope(bid_price).alias(f'bid_price_trend_l{level}')
            )
            
            # Ask price trend: slope(AskPrice ORDER BY TimestampNanoseconds)
            features.append(
                self._slope(ask_price).alias(f'ask_price_trend_l{level}')
            )
            
            # Mid price trend: slope((BidPrice + AskPrice) / 2 ORDER BY TimestampNanoseconds)
            # Calculate mid price first, then slope
            features.append(
                self._slope_mid_price(bid_price, ask_price).alias(f'mid_price_trend_l{level}')
            )
            
            # Bid quantity trend: slope(BidQuantity ORDER BY TimestampNanoseconds)
            features.append(
                self._slope(bid_qty).alias(f'bid_quantity_trend_l{level}')
            )
            
            # Ask quantity trend: slope(AskQuantity ORDER BY TimestampNanoseconds)
            features.append(
                self._slope(ask_qty).alias(f'ask_quantity_trend_l{level}')
            )
            
            # Bid volume trend: slope(BidPrice * BidQuantity ORDER BY TimestampNanoseconds)
            features.append(
                self._slope_product(bid_price, bid_qty).alias(f'bid_volume_trend_l{level}')
            )
            
            # Ask volume trend: slope(AskPrice * AskQuantity ORDER BY TimestampNanoseconds)
            features.append(
                self._slope_product(ask_price, ask_qty).alias(f'ask_volume_trend_l{level}')
            )
        
        return features
    
    def _section8_trend_vol_features(self, df: pl.LazyFrame) -> List[pl.Expr]:
        """Section 8: Trend Vol Features."""
        features = []
        
        # Trend volatility (MSE) for bid/ask/mid prices, quantities, volumes for levels 1-10
        for level in range(1, 11):
            bid_price = f'BidPrice{level}'
            ask_price = f'AskPrice{level}'
            bid_qty = f'BidQuantity{level}'
            ask_qty = f'AskQuantity{level}'
            
            # Bid price trend volatility: mse(BidPrice ORDER BY TimestampNanoseconds) / median(BidPrice)
            features.append(
                (self._mse_trend(bid_price) / pl.col(bid_price).median()).alias(f'bid_price_trend_vol_l{level}')
            )
            
            # Ask price trend volatility: mse(AskPrice ORDER BY TimestampNanoseconds) / median(AskPrice)
            features.append(
                (self._mse_trend(ask_price) / pl.col(ask_price).median()).alias(f'ask_price_trend_vol_l{level}')
            )
            
            # Mid price trend volatility: mse((BidPrice + AskPrice) / 2 ORDER BY TimestampNanoseconds) / median((BidPrice + AskPrice) / 2)
            mid_price = (pl.col(bid_price) + pl.col(ask_price)) / 2
            features.append(
                (self._mse_trend_mid_price(bid_price, ask_price) / mid_price.median()).alias(f'mid_price_trend_vol_l{level}')
            )
            
            # Bid quantity trend volatility: mse(BidQuantity ORDER BY TimestampNanoseconds) / median(BidQuantity)
            features.append(
                (self._mse_trend(bid_qty) / pl.col(bid_qty).median()).alias(f'bid_quantity_trend_vol_l{level}')
            )
            
            # Ask quantity trend volatility: mse(AskQuantity ORDER BY TimestampNanoseconds) / median(AskQuantity)
            features.append(
                (self._mse_trend(ask_qty) / pl.col(ask_qty).median()).alias(f'ask_quantity_trend_vol_l{level}')
            )
            
            # Bid volume trend volatility: mse(BidPrice * BidQuantity ORDER BY TimestampNanoseconds) / median(BidPrice * BidQuantity)
            bid_volume = pl.col(bid_price) * pl.col(bid_qty)
            features.append(
                (self._mse_trend_product(bid_price, bid_qty) / bid_volume.median()).alias(f'bid_volume_trend_vol_l{level}')
            )
            
            # Ask volume trend volatility: mse(AskPrice * AskQuantity ORDER BY TimestampNanoseconds) / median(AskPrice * AskQuantity)
            ask_volume = pl.col(ask_price) * pl.col(ask_qty)
            features.append(
                (self._mse_trend_product(ask_price, ask_qty) / ask_volume.median()).alias(f'ask_volume_trend_vol_l{level}')
            )
        
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
    
    def feature_computation(self, data: pl.LazyFrame) -> pl.LazyFrame:
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