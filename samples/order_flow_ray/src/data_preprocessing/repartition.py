"""Repartition step for reorganizing data by partition key."""
from typing import List, Any, Tuple
import polars as pl


class Repartition:
    """Repartition data by adding partition key to S3 path structure."""
    
    def __init__(self, partition_column: str, max_retries: int, log_interval: int):
        """Initialize repartition with partition column and retry configuration.
        
        Args:
            partition_column: Column name to partition by (e.g., 'Ticker', 'Model', 'Strategy')
            max_retries: Maximum retry attempts for failed files
            log_interval: Log progress every N partitions
        """
        self.partition_column = partition_column
        self.max_retries = max_retries
        self.log_interval = log_interval
    
    def discover_files(self, data_access, input_path: str, file_sort_order: str) -> List[Tuple[str, int]]:
        """Discover parquet files for repartitioning from any pipeline stage.
        
        Args:
            data_access: Data access instance
            input_path: S3 path to input data (can be normalized, features, etc.)
            file_sort_order: Sort order ('asc' or 'desc')
        
        Returns:
            List of (file_path, size) tuples sorted by file_sort_order
        """
        files_with_sizes = data_access.list_files(input_path)
        parquet_files = [(fp, fs) for fp, fs in files_with_sizes if fp.endswith('.parquet')]
        
        reverse = (file_sort_order == 'desc')
        parquet_files.sort(key=lambda x: x[0], reverse=reverse)
        
        return parquet_files
    
    def repartition_trades(self, df: pl.LazyFrame, source_path: str, data_access, output_path_base: str) -> list[dict]:
        """Repartition trades data by unique partition combinations.
        
        Args:
            df: Trades data (LazyFrame)
            source_path: Source file path for logging
            data_access: Data access instance for writing
            output_path_base: Base output path without partition key
            
        Returns:
            List of result dicts for each partition written
        """
        print(f"[REPARTITION] Processing trades file: {source_path}")
        
        # Add TickerPrefix column (first letter of Ticker)
        df = df.with_columns(pl.col('Ticker').str.slice(0, 1).str.to_uppercase().alias('TickerPrefix'))
        
        partition_keys = df.select(['TradeDate', 'TickerPrefix', 'ISOExchangeCode', 'MIC', 'OPOL', 'ExecutionVenue']).unique().collect(streaming=True)
        total_partitions = len(partition_keys)
        print(f"[REPARTITION] Found {total_partitions} unique partitions in {source_path}")
        
        results = []
        for idx, row in enumerate(partition_keys.iter_rows(named=True), 1):
            import time
            partition_start = time.time()
            
            partition_df = df.filter(
                (pl.col('TradeDate') == row['TradeDate']) &
                (pl.col('TickerPrefix') == row['TickerPrefix']) &
                (pl.col('ISOExchangeCode') == row['ISOExchangeCode']) &
                (pl.col('MIC') == row['MIC']) &
                (pl.col('OPOL') == row['OPOL']) &
                (pl.col('ExecutionVenue') == row['ExecutionVenue'])
            )
            
            # Build partition path: TickerPrefix/ISOExchangeCode/MIC/OPOL/ExecutionVenue
            partition_path = f"{row['TickerPrefix']}/{row['ISOExchangeCode']}/{row['MIC']}/{row['OPOL']}/{row['ExecutionVenue']}"
            output_path = f"{output_path_base}/{partition_path}/{source_path.split('/')[-1]}"
            
            data_access.write(partition_df, output_path)
            row_count = partition_df.select(pl.len()).collect().item()
            
            partition_time = time.time() - partition_start
            
            # Log every N partitions
            if idx % self.log_interval == 0 or idx == total_partitions:
                print(f"[REPARTITION] Progress: {idx}/{total_partitions} partitions written. Last: {output_path} ({row_count} rows, {partition_time:.2f}s)")
            
            try:
                output_size_mb = data_access.get_file_size(output_path) / (1024 ** 2)
            except:
                output_size_mb = 0
            
            results.append({
                'output_path': output_path,
                'row_count': row_count,
                'output_size_mb': output_size_mb,
                'partition_value': partition_path
            })
        
        print(f"[REPARTITION] Completed {source_path}: {total_partitions} partitions written to {output_path_base}")
        return results
    
    def repartition_l2q(self, df: pl.LazyFrame, source_path: str, data_access, output_path_base: str) -> list[dict]:
        """Repartition L2Q data by unique partition combinations.
        
        Args:
            df: L2Q data (LazyFrame)
            source_path: Source file path for logging
            data_access: Data access instance for writing
            output_path_base: Base output path without partition key
            
        Returns:
            List of result dicts for each partition written
        """
        print(f"[REPARTITION] Processing L2Q file: {source_path}")
        
        # Add TickerPrefix column (first letter of Ticker)
        df = df.with_columns(pl.col('Ticker').str.slice(0, 1).str.to_uppercase().alias('TickerPrefix'))
        
        partition_keys = df.select(['TradeDate', 'TickerPrefix', 'ISOExchangeCode', 'MIC']).unique().collect(streaming=True)
        total_partitions = len(partition_keys)
        print(f"[REPARTITION] Found {total_partitions} unique partitions in {source_path}")
        
        results = []
        for idx, row in enumerate(partition_keys.iter_rows(named=True), 1):
            import time
            partition_start = time.time()
            
            partition_df = df.filter(
                (pl.col('TradeDate') == row['TradeDate']) &
                (pl.col('TickerPrefix') == row['TickerPrefix']) &
                (pl.col('ISOExchangeCode') == row['ISOExchangeCode']) &
                (pl.col('MIC') == row['MIC'])
            )
            
            # Build partition path: TickerPrefix/ISOExchangeCode/MIC
            partition_path = f"{row['TickerPrefix']}/{row['ISOExchangeCode']}/{row['MIC']}"
            output_path = f"{output_path_base}/{partition_path}/{source_path.split('/')[-1]}"
            
            data_access.write(partition_df, output_path)
            row_count = partition_df.select(pl.len()).collect().item()
            
            partition_time = time.time() - partition_start
            
            # Log every N partitions
            if idx % self.log_interval == 0 or idx == total_partitions:
                print(f"[REPARTITION] Progress: {idx}/{total_partitions} partitions written. Last: {output_path} ({row_count} rows, {partition_time:.2f}s)")
            
            try:
                output_size_mb = data_access.get_file_size(output_path) / (1024 ** 2)
            except:
                output_size_mb = 0
            
            results.append({
                'output_path': output_path,
                'row_count': row_count,
                'output_size_mb': output_size_mb,
                'partition_value': partition_path
            })
        
        print(f"[REPARTITION] Completed {source_path}: {total_partitions} partitions written to {output_path_base}")
        return results
    
    def repartition(self, df: pl.LazyFrame, source_path: str, data_access, output_path_base: str) -> list[dict]:
        """Repartition data - writes each partition immediately.
        
        Args:
            df: Input data from any pipeline stage
            source_path: Source file path for logging
            data_access: Data access instance for writing
            output_path_base: Base output path without partition key
            
        Returns:
            List of result dicts for each partition written
        """
        data_type = 'level2q' if 'level2q' in source_path else 'trades'
        
        if data_type == 'trades':
            return self.repartition_trades(df, source_path, data_access, output_path_base)
        else:
            return self.repartition_l2q(df, source_path, data_access, output_path_base)
    
    def get_failed_items(self, results: List[Any]) -> List[Tuple[str, int]]:
        """Extract failed items for retry."""
        failed = []
        for r in results:
            if isinstance(r, dict) and 'error' in r.get('message', '').lower():
                failed.append((r['input_path'], r['size_gb']))
        return failed
    
    def failure_extraction(self, results: List[Any]) -> List[Any]:
        """Extract failure results."""
        return [r for r in results if isinstance(r, dict) and 'error' in r.get('message', '').lower()]
