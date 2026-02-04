"""Order flow feature engineering pipeline."""
import ray
import polars as pl
from math import ceil
from typing import List, Dict, Any, Tuple
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
    
    def process_files(self, normalized_files: List[Tuple[str, float]]) -> List[Dict]:
        """Process normalized files to generate features.
        
        Args:
            normalized_files: List of (file_path, size_gb) tuples
            
        Returns:
            List of processing results
        """
        self.initialize()
        
        # Submit Ray tasks for each file
        futures = []
        for file_path, file_size in normalized_files:
            future = self._submit_feature_task(file_path, file_size)
            futures.append(future)
        
        # Wait for all tasks to complete
        results = ray.get(futures)
        return results
    
    def _submit_feature_task(self, file_path: str, file_size: float):
        """Submit feature engineering task to Ray."""
        
        # Dynamic resource allocation (reuse pattern from pipeline.py)
        if self.config.ray.flat_core_count is not None:
            num_cpus = self.config.ray.flat_core_count
            memory_gb = num_cpus * self.config.ray.memory_per_core_gb
        else:
            memory_bytes = int(file_size * self.config.ray.memory_multiplier)
            memory_gb = memory_bytes / (1024 ** 3)
            num_cpus = ceil(memory_gb / self.config.ray.memory_per_core_gb) + self.config.ray.cpu_buffer
        
        @ray.remote(num_cpus=num_cpus, max_retries=0)
        def process_file(fp: str, fs: float, region: str, profile: str, bar_duration_ms: int) -> Dict:
            """Ray remote function for feature engineering."""
            try:
                from data_preprocessing.data_access.factory import DataAccessFactory
                from feature_engineering.order_flow import L2QFeatureEngineering, TradeFeatureEngineering
                
                # Initialize data access
                data_access = DataAccessFactory.create('s3', region=region, profile_name=profile)
                
                # Read normalized data
                df = data_access.read(fp)
                
                # Determine data type from path
                data_type = 'level2q' if 'level2q' in fp else 'trades'
                
                # Apply appropriate feature engineering
                if data_type == 'level2q':
                    feature_eng = L2QFeatureEngineering(bar_duration_ms=bar_duration_ms)
                else:
                    feature_eng = TradeFeatureEngineering(bar_duration_ms=bar_duration_ms)
                
                # Process features
                features_df = feature_eng.feature_computation(df)
                
                # Generate output path
                output_path = fp.replace('/normalized/', '/features/').replace('.parquet', f'_features_{bar_duration_ms}ms.parquet')
                
                # Write features
                data_access.write(features_df, output_path)
                
                # Get row count
                row_count = features_df.select(pl.len()).collect().item()
                
                return {
                    'input_path': fp,
                    'output_path': output_path,
                    'data_type': data_type,
                    'row_count': row_count,
                    'bar_duration_ms': bar_duration_ms,
                    'message': 'success'
                }
                
            except Exception as e:
                return {
                    'input_path': fp,
                    'output_path': None,
                    'data_type': data_type if 'data_type' in locals() else 'unknown',
                    'row_count': 0,
                    'bar_duration_ms': bar_duration_ms,
                    'message': str(e)
                }
        
        # Submit task
        return process_file.remote(
            file_path, 
            file_size, 
            self.config.region, 
            self.config.profile_name,
            self.config.processing.feature_engineering.bar_duration_ms
        )
    
    def get_failed_items(self, results: List[Dict]) -> List[Tuple[str, float]]:
        """Extract failed items for retry (reuse pattern from pipeline.py)."""
        failed = []
        for result in results:
            if result['message'] != 'success':
                # Extract file size from result or estimate
                file_size = result.get('file_size_gb', 1.0)  # Default 1GB if unknown
                failed.append((result['input_path'], file_size))
        return failed
    
    def shutdown(self):
        """Shutdown Ray."""
        if ray.is_initialized():
            ray.shutdown()