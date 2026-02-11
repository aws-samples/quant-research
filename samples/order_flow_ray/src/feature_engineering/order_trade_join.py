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
    
    def discover_files(self, data_access, features_path: str, sort_order: str, discovery_mode: str) -> Tuple[List[Tuple[str, str, float]], List[str], List[str]]:
        """Discover both L2Q and Trade files for pairing.
        
        Args:
            data_access: Data access instance
            features_path: Base path to features
            sort_order: Sort order ('asc' or 'desc')
            discovery_mode: Discovery mode ('asynch' or 'sync')
            
        Returns:
            Tuple of (paired_files, unmatched_l2q, unmatched_trade)
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
        
        return paired_files, unmatched_l2q, unmatched_trade
    
    def pair_files(self, l2q_files: List[Tuple[str, float]], trade_files: List[Tuple[str, float]]) -> Tuple[List[Tuple[str, str, float]], List[str], List[str]]:
        """Pair L2Q and Trade files by path matching.
        
        Args:
            l2q_files: List of (l2q_path, size) tuples
            trade_files: List of (trade_path, size) tuples
            
        Returns:
            Tuple of (paired_files, unmatched_l2q, unmatched_trade)
        """
        # Create lookup dict for trade files by converting trade paths to l2q format
        trade_dict = {}
        for trade_path, trade_size in trade_files:
            # Convert /trades/ to /level2q/ to create matching key
            l2q_key = trade_path.replace('/trades/', '/level2q/')
            trade_dict[l2q_key] = (trade_path, trade_size)
        
        paired_files = []
        unmatched_l2q = []
        
        # Match L2Q files with Trade files
        for l2q_path, l2q_size in l2q_files:
            if l2q_path in trade_dict:
                trade_path, trade_size = trade_dict[l2q_path]
                max_size = max(l2q_size, trade_size)
                paired_files.append((l2q_path, trade_path, max_size))
                del trade_dict[l2q_path]  # Remove matched
            else:
                unmatched_l2q.append(l2q_path)
        
        # Remaining trade files are unmatched
        unmatched_trade = [trade_path for trade_path, _ in trade_dict.values()]
        
        return paired_files, unmatched_l2q, unmatched_trade
    
    def group_file_pairs_for_processing(self, file_pairs: List[Tuple[str, str, float]]) -> List[List[Tuple[str, str, float]]]:
        """Group file pairs for processing.
        
        Args:
            file_pairs: List of (l2q_path, trade_path, max_size) tuples
            
        Returns:
            List of file pair groups
        """
        # TODO: Implement grouping logic
        pass
    
    def join_features(self, l2q_path: str, trade_path: str) -> pl.LazyFrame:
        """Join L2Q and Trade features.
        
        Args:
            l2q_path: Path to L2Q features
            trade_path: Path to Trade features
            
        Returns:
            Combined features LazyFrame
        """
        # TODO: Implement join logic
        pass
    
    def get_failed_items(self, results: List[dict]) -> List[List[Tuple[str, str, float]]]:
        """Extract failed file pairs for retry.
        
        Args:
            results: List of processing results
            
        Returns:
            List of failed file pair groups
        """
        # TODO: Implement failure extraction
        pass