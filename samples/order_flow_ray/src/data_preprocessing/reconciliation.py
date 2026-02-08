"""Reconciliation step for validating data consistency between normalized and repartitioned data."""
from typing import List, Tuple, Any
import polars as pl
from datetime import datetime


class Reconciliation:
    """Reconcile data between normalized and repartitioned locations."""
    
    def __init__(self, max_retries: int = 3):
        """Initialize reconciliation with retry configuration.
        
        Args:
            max_retries: Maximum retry attempts for failed reconciliations
        """
        self.max_retries = max_retries
    
    def discover_files(self, data_access, normalized_path: str, file_sort_order: str) -> List[Tuple[str, str]]:
        """Discover unique (date, data_type) combinations from normalized path.
        
        Args:
            data_access: Data access instance
            normalized_path: S3 path to normalized data
            file_sort_order: Sort order ('asc' or 'desc')
        
        Returns:
            List of (date, data_type) tuples
        """
        files_with_sizes = data_access.list_files(normalized_path)
        parquet_files = [fp for fp, _ in files_with_sizes if fp.endswith('.parquet')]
        
        # Extract (date, data_type) from path: YYYY/MM/DD/{data_type}/AMERICAS/{filename}
        date_type_set = set()
        for fp in parquet_files:
            parts = fp.split('/')
            if len(parts) >= 5:
                # Find normalized_path in the file path and extract relative path
                if normalized_path.rstrip('/') in fp:
                    relative = fp.split(normalized_path.rstrip('/') + '/')[-1]
                    rel_parts = relative.split('/')
                    if len(rel_parts) >= 4:
                        year, month, day = rel_parts[0], rel_parts[1], rel_parts[2]
                        data_type = rel_parts[3]
                        date_str = f"{year}-{month}-{day}"
                        date_type_set.add((date_str, data_type))
        
        date_type_list = sorted(list(date_type_set), reverse=(file_sort_order == 'desc'))
        return date_type_list
    
    def reconcile(self, date: str, data_type: str, data_access, normalized_path: str, repartitioned_path: str) -> dict:
        """Reconcile data for a specific date and data_type.
        
        Args:
            date: Date string (YYYY-MM-DD)
            data_type: Data type ('trades' or 'level2q')
            data_access: Data access instance
            normalized_path: Base path to normalized data
            repartitioned_path: Base path to repartitioned data
            
        Returns:
            Dict with reconciliation results including mismatches
        """
        print(f"[RECONCILIATION] Processing {date} / {data_type}")
        
        # Build paths
        date_parts = date.split('-')
        year, month, day = date_parts[0], date_parts[1], date_parts[2]
        norm_pattern = f"{normalized_path.rstrip('/')}/{year}/{month}/{day}/{data_type}/AMERICAS/*.parquet"
        repart_pattern = f"{repartitioned_path.rstrip('/')}/{year}/{month}/{day}/{data_type}/AMERICAS/**/*.parquet"
        
        # Define grouping keys based on data type
        if data_type == 'trades':
            group_keys = ['TradeDate', 'Ticker', 'ISOExchangeCode', 'MIC', 'OPOL', 'ExecutionVenue']
        else:  # level2q
            group_keys = ['TradeDate', 'Ticker', 'ISOExchangeCode', 'MIC']
        
        # Read and aggregate normalized data
        norm_df = pl.scan_parquet(norm_pattern, storage_options=data_access.get_storage_options())
        norm_counts = norm_df.group_by(group_keys).agg(pl.len().alias('normalized_count')).collect(streaming=True)
        
        # Read and aggregate repartitioned data
        repart_df = pl.scan_parquet(repart_pattern, storage_options=data_access.get_storage_options())
        repart_counts = repart_df.group_by(group_keys).agg(pl.len().alias('repartitioned_count')).collect(streaming=True)
        
        # Full outer join to find all combinations
        comparison = norm_counts.join(repart_counts, on=group_keys, how='full', coalesce=True)
        
        # Fill nulls with 0 for counts
        comparison = comparison.with_columns([
            pl.col('normalized_count').fill_null(0),
            pl.col('repartitioned_count').fill_null(0)
        ])
        
        # Calculate difference and status
        comparison = comparison.with_columns([
            (pl.col('repartitioned_count') - pl.col('normalized_count')).alias('difference'),
            pl.when(pl.col('normalized_count') == 0)
              .then(pl.lit('missing_in_normalized'))
              .when(pl.col('repartitioned_count') == 0)
              .then(pl.lit('missing_in_repartitioned'))
              .when(pl.col('normalized_count') != pl.col('repartitioned_count'))
              .then(pl.lit('count_mismatch'))
              .otherwise(pl.lit('match'))
              .alias('status')
        ])
        
        # Filter for mismatches only
        mismatches = comparison.filter(pl.col('status') != 'match')
        
        # Add metadata columns
        mismatches = mismatches.with_columns([
            pl.lit(data_type).alias('DataType')
        ])
        
        # Reorder columns
        if data_type == 'trades':
            column_order = ['TradeDate', 'DataType', 'Ticker', 'ISOExchangeCode', 'MIC', 'OPOL', 'ExecutionVenue', 
                          'normalized_count', 'repartitioned_count', 'difference', 'status']
        else:
            column_order = ['TradeDate', 'DataType', 'Ticker', 'ISOExchangeCode', 'MIC', 
                          'normalized_count', 'repartitioned_count', 'difference', 'status']
        
        mismatches = mismatches.select(column_order)
        
        # Calculate summary stats
        total_groups = len(comparison)
        matched_groups = len(comparison.filter(pl.col('status') == 'match'))
        mismatched_groups = len(mismatches)
        
        print(f"[RECONCILIATION] {date} / {data_type}: {matched_groups} matched, {mismatched_groups} mismatched out of {total_groups} groups")
        
        return {
            'date': date,
            'data_type': data_type,
            'total_groups': total_groups,
            'matched_groups': matched_groups,
            'mismatched_groups': mismatched_groups,
            'mismatches': mismatches,
            'message': 'success'
        }
    
    def get_failed_items(self, results: List[Any]) -> List[Tuple[str, str]]:
        """Extract failed items for retry.
        
        Args:
            results: List of reconciliation results
            
        Returns:
            List of (date, data_type) tuples that failed
        """
        failed = []
        for r in results:
            if isinstance(r, dict) and r.get('message') != 'success':
                failed.append((r['date'], r['data_type']))
        return failed
    
    def failure_extraction(self, results: List[Any]) -> List[Any]:
        """Extract failure results.
        
        Args:
            results: List of reconciliation results
            
        Returns:
            List of failed results
        """
        return [r for r in results if isinstance(r, dict) and r.get('message') != 'success']
