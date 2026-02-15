"""Order flow feature engineering pipeline."""
import ray
import polars as pl
from math import ceil
from typing import List, Dict, Any, Tuple
from collections import defaultdict
from data_preprocessing.data_access.factory import DataAccessFactory
from feature_engineering.order_flow import L2QFeatureEngineering, TradeFeatureEngineering

class OrderFlowFeatureEngineering:
    """Ray-based distributed order flow feature engineering."""
    
    def __init__(self, config):
        """Initialize with pipeline config."""
        self.config = config
        self.data_access = None
    
    def initialize(self):
        """Initialize Ray and data access."""
        if not ray.is_initialized():
            ray.init()
        
        # Reuse data access factory from data_preprocessing
        self.data_access = DataAccessFactory.create(
            "s3",
            region=self.config.region,
            profile_name=self.config.profile_name
        )
    
    def _group_files_by_day_exchange(self, normalized_files: List[Tuple[str, float]]) -> Dict[str, List[Tuple[str, float]]]:
        """Group files by day and exchange for combined processing.
        
        Args:
            normalized_files: List of (file_path, size_gb) tuples
            
        Returns:
            Dict mapping 'date_exchange' to list of (file_path, size_gb) tuples
        """
        groups = defaultdict(list)
        
        for file_path, file_size in normalized_files:
            # Extract date and exchange from file path
            # Expected format: .../YYYY/MM/DD/exchange/data_type/file.parquet
            path_parts = file_path.split('/')
            
            # Find date components (YYYY/MM/DD)
            date_idx = None
            for i, part in enumerate(path_parts):
                if len(part) == 4 and part.isdigit() and 2020 <= int(part) <= 2030:
                    date_idx = i
                    break
            
            if date_idx and date_idx + 2 < len(path_parts):
                year = path_parts[date_idx]
                month = path_parts[date_idx + 1]
                day = path_parts[date_idx + 2]
                exchange = path_parts[date_idx + 3] if date_idx + 3 < len(path_parts) else 'unknown'
                
                group_key = f"{year}{month}{day}_{exchange}"
                groups[group_key].append((file_path, file_size))
            else:
                # Fallback: use filename as group key
                filename = path_parts[-1]
                group_key = filename.split('.')[0]
                groups[group_key].append((file_path, file_size))
        
        return dict(groups)
    
    def process_files(self, normalized_files: List[Tuple[str, float]]) -> List[Dict]:
        """Process normalized files to generate features.
        
        Args:
            normalized_files: List of (file_path, size_gb) tuples
            
        Returns:
            List of processing results
        """
        self.initialize()
        
        # Group files by day and exchange
        file_groups = self._group_files_by_day_exchange(normalized_files)
        
        # Submit Ray tasks for each group
        futures = []
        for group_key, group_files in file_groups.items():
            future = self._submit_group_task(group_key, group_files)
            futures.append(future)
        
        # Wait for all tasks to complete
        results = ray.get(futures)
        
        # Flatten results from groups
        flattened_results = []
        for group_result in results:
            if isinstance(group_result, list):
                flattened_results.extend(group_result)
            else:
                flattened_results.append(group_result)
        
        return flattened_results
    
    def _submit_group_task(self, group_key: str, group_files: List[Tuple[str, float]]):
        """Submit grouped feature engineering task to Ray."""
        
        # Calculate total size for resource allocation
        total_size = sum(size for _, size in group_files)
        
        # Dynamic resource allocation (reuse pattern from pipeline.py)
        if self.config.ray.flat_core_count is not None:
            num_cpus = self.config.ray.flat_core_count
            memory_gb = num_cpus * self.config.ray.memory_per_core_gb
        else:
            memory_bytes = int(total_size * self.config.ray.memory_multiplier)
            memory_gb = memory_bytes / (1024 ** 3)
            num_cpus = ceil(memory_gb / self.config.ray.memory_per_core_gb) + self.config.ray.cpu_buffer
        
        @ray.remote(num_cpus=num_cpus, max_retries=0)
        def process_group(gk: str, gf: List[Tuple[str, float]], region: str, profile: str, bar_duration_ms: int) -> List[Dict]:
            """Ray remote function for grouped feature engineering."""
            try:
                from data_preprocessing.data_access.factory import DataAccessFactory
                from feature_engineering.order_flow import L2QFeatureEngineering, TradeFeatureEngineering
                
                # Initialize data access
                data_access = DataAccessFactory.create('s3', region=region, profile_name=profile)
                
                # Initialize feature engineering instances
                l2q_eng = L2QFeatureEngineering(bar_duration_ms=bar_duration_ms)
                trade_eng = TradeFeatureEngineering(bar_duration_ms=bar_duration_ms)
                
                results = []
                
                # Process each file in the group sequentially
                for file_path, file_size in gf:
                    try:
                        # Read normalized data
                        df = data_access.read(file_path)
                        
                        # Determine data type from path
                        data_type = 'level2q' if 'level2q' in file_path else 'trades'
                        
                        # Apply appropriate feature engineering
                        if data_type == 'level2q':
                            features_df = l2q_eng.feature_computation(df)
                        else:
                            features_df = trade_eng.feature_computation(df)
                        
                        # Generate output path
                        output_path = file_path.replace('/normalized/', '/features/').replace('.parquet', f'_features_{bar_duration_ms}ms.parquet')
                        
                        # Write features
                        data_access.write(features_df, output_path)
                        
                        # Get row count
                        if isinstance(features_df, pl.LazyFrame):
                            row_count = features_df.select(pl.len()).collect().item()
                        else:
                            row_count = features_df.select(pl.len()).item()
                        
                        results.append({
                            'group_key': gk,
                            'input_path': file_path,
                            'output_path': output_path,
                            'data_type': data_type,
                            'row_count': row_count,
                            'bar_duration_ms': bar_duration_ms,
                            'message': 'success'
                        })
                        
                    except Exception as e:
                        results.append({
                            'group_key': gk,
                            'input_path': file_path,
                            'output_path': None,
                            'data_type': data_type if 'data_type' in locals() else 'unknown',
                            'row_count': 0,
                            'bar_duration_ms': bar_duration_ms,
                            'message': str(e)
                        })
                
                return results
                
            except Exception as e:
                # Return error for entire group
                return [{
                    'group_key': gk,
                    'input_path': 'group_error',
                    'output_path': None,
                    'data_type': 'unknown',
                    'row_count': 0,
                    'bar_duration_ms': bar_duration_ms,
                    'message': f'Group processing error: {str(e)}'
                }]
        
        # Submit task
        return process_group.remote(
            group_key,
            group_files,
            self.config.region, 
            self.config.profile_name,
            self.config.processing.feature_engineering.bar_duration_ms
        )
    
    def get_failed_items(self, results: List[Dict]) -> List[Tuple[str, float]]:
        """Extract failed items for retry (reuse pattern from pipeline.py)."""
        failed = []
        for result in results:
            if result['message'] != 'success' and result['input_path'] != 'group_error':
                # Extract file size from result or estimate
                file_size = result.get('file_size_gb', 1.0)  # Default 1GB if unknown
                failed.append((result['input_path'], file_size))
        return failed
    
    def shutdown(self):
        """Shutdown Ray."""
        if ray.is_initialized():
            ray.shutdown()