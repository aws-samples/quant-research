"""Feature join for L2Q and Trade features."""
from typing import List, Tuple
import polars as pl


class OrderTradeFeatureJoin:
    """Join L2Q and Trade features into unified dataset."""
    
    def __init__(self, bar_duration_ms: int = 250, max_retries: int = 3):
        """Initialize feature join.
        
        Args:
            bar_duration_ms: Bar duration in milliseconds
            max_retries: Maximum retry attempts
        """
        self.bar_duration_ms = bar_duration_ms
        self.max_retries = max_retries
    
    def discover_l2q_files(self, data_access, features_path: str, sort_order: str) -> List[Tuple[str, float]]:
        """Discover L2Q feature files.
        
        Args:
            data_access: Data access instance
            features_path: Base path to features
            sort_order: Sort order ('asc' or 'desc')
            
        Returns:
            List of (file_path, file_size_gb) tuples for L2Q files
        """
        all_files = data_access.discover_files_asynch(features_path, sort_order)
        return [(path, size) for path, size in all_files if '/level2q/' in path]
    
    def discover_trade_files(self, data_access, features_path: str, sort_order: str) -> List[Tuple[str, float]]:
        """Discover Trade feature files.
        
        Args:
            data_access: Data access instance
            features_path: Base path to features
            sort_order: Sort order ('asc' or 'desc')
            
        Returns:
            List of (file_path, file_size_gb) tuples for Trade files
        """
        all_files = data_access.discover_files_asynch(features_path, sort_order)
        return [(path, size) for path, size in all_files if '/trades/' in path]
    
    def discover_files(self, data_access, features_path: str, sort_order: str, discovery_mode: str) -> Tuple[List[Tuple[str, str, float]], List[str], List[str], List[Tuple[str, float]]]:
        """Discover both L2Q and Trade files for pairing.
        
        Args:
            data_access: Data access instance
            features_path: Base path to features
            sort_order: Sort order ('asc' or 'desc')
            discovery_mode: Discovery mode ('asynch' or 'sync')
            
        Returns:
            Tuple of (paired_files, unmatched_l2q, unmatched_trade, all_files)
        """
        print(f"Discovering L2Q and Trade feature files in: {features_path}")
        
        # Discover all files once
        all_files = data_access.discover_files_asynch(features_path, sort_order)
        
        # Split into L2Q and Trade files
        l2q_files = [(path, size) for path, size in all_files if '/level2q/' in path]
        trade_files = [(path, size) for path, size in all_files if '/trades/' in path]
        
        print(f"Found {len(l2q_files)} L2Q files and {len(trade_files)} Trade files")
        
        # Pair files and return all results
        paired_files, unmatched_l2q, unmatched_trade = self.pair_files(l2q_files, trade_files)
        
        if unmatched_l2q:
            print(f"Warning: {len(unmatched_l2q)} L2Q files without matching Trade files")
        if unmatched_trade:
            print(f"Warning: {len(unmatched_trade)} Trade files without matching L2Q files")
        
        return paired_files, unmatched_l2q, unmatched_trade, all_files
    
    def pair_files(self, l2q_files: List[Tuple[str, float]], trade_files: List[Tuple[str, float]]) -> Tuple[List[Tuple[str, str, float]], List[str], List[str]]:
        """Pair L2Q and Trade files by path matching.
        
        Args:
            l2q_files: List of (l2q_path, size) tuples
            trade_files: List of (trade_path, size) tuples
            
        Returns:
            Tuple of (paired_files, unmatched_l2q, unmatched_trade)
        """
        def extract_key(path):
            """Extract matching key including prefix from path."""
            parts = path.split('/')
            if len(parts) >= 10:
                # Extract from: .../2024/04/04/trades/AMERICAS/A/trades-@SIP-20240404.parquet
                year = parts[-7]  # 2024
                month = parts[-6]  # 04
                day = parts[-5]  # 04
                feature_type = parts[-4]  # trades or level2q
                region = parts[-3]  # AMERICAS
                prefix = parts[-2]  # A
                filename = parts[-1]  # trades-@SIP-20240404.parquet
                # Extract ticker from filename
                if filename.startswith('trades-'):
                    ticker = filename[7:].split('-')[0]  # @SIP
                else:
                    ticker = filename.split('-')[0]
                return f"repartitioned_v3/{year}/{month}/{day}/filler/{region}/{prefix}/{ticker}"
            return None
        
        # Convert to Polars DataFrames
        l2q_df = pl.DataFrame([
            {"path": path, "size": size, "match_key": extract_key(path)}
            for path, size in l2q_files
        ]).filter(pl.col("match_key").is_not_null())
        
        trade_df = pl.DataFrame([
            {"path": path, "size": size, "match_key": extract_key(path)}
            for path, size in trade_files
        ]).filter(pl.col("match_key").is_not_null())
        
        # Join on match_key
        joined = l2q_df.join(trade_df, on="match_key", how="inner", suffix="_trade")
        
        # Extract results
        paired_files = [
            (row["path"], row["path_trade"], max(row["size"], row["size_trade"]))
            for row in joined.to_dicts()
        ]
        
        matched_keys = set(joined["match_key"].to_list())
        unmatched_l2q = [path for path, _ in l2q_files if extract_key(path) not in matched_keys]
        unmatched_trade = [path for path, _ in trade_files if extract_key(path) not in matched_keys]
        
        return paired_files, unmatched_l2q, unmatched_trade
    
    def group_file_pairs_for_processing(self, file_pairs: List[Tuple[str, str, float]]) -> List[List[Tuple[str, str, float]]]:
        """Group file pairs for processing.
        
        Args:
            file_pairs: List of (l2q_path, trade_path, max_size) tuples
            
        Returns:
            List of file pair groups
        """
        if not file_pairs:
            return []
        
        # Simple grouping - each pair is its own group for now
        # Can be enhanced later with size-based grouping
        return [[pair] for pair in file_pairs]
    
    def join_features(self, l2q_path: str, trade_path: str) -> pl.LazyFrame:
        """Join L2Q and Trade features.
        
        Args:
            l2q_path: Path to L2Q features
            trade_path: Path to Trade features
            
        Returns:
            Combined features LazyFrame
        """
        # Read both feature files
        l2q_features = pl.scan_parquet(l2q_path)
        trade_features = pl.scan_parquet(trade_path)
        
        # Join on common columns (assuming BarId exists in both)
        joined = l2q_features.join(
            trade_features,
            on=['BarId'],
            how='inner',
            suffix='_trade'
        )
        
        return joined
    
    def get_failed_items(self, results: List[dict]) -> List[List[Tuple[str, str, float]]]:
        """Extract failed file pairs for retry.
        
        Args:
            results: List of processing results
            
        Returns:
            List of failed file pair groups
        """
        failed = [r for r in results if r['message'] != 'success']
        failed_pairs = []
        
        for result in failed:
            input_path = result.get('input_path', '')
            if ' + ' in input_path:
                l2q_path, trade_path = input_path.split(' + ', 1)
                size_gb = result.get('size_gb', 1.0)
                failed_pairs.append((l2q_path, trade_path, size_gb * (1024**3)))
        
        # Re-group the failed pairs
        if failed_pairs:
            return self.group_file_pairs_for_processing(failed_pairs)
        else:
            return []