"""Pipeline orchestrator for Ray-based BMLL processing."""
import ray
import polars as pl
from typing import Any
from .config import PipelineConfig
from data_preprocessing.data_access.factory import DataAccessFactory


class Pipeline:
    """Orchestrates data processing pipeline using Ray."""
    
    def __init__(self, config: PipelineConfig):
        """Initialize pipeline.
        
        Args:
            config: Pipeline configuration
        """
        self.config = config
        self.data_access = None
        
    def initialize(self):
        """Initialize Ray and data access."""
        if not ray.is_initialized():
            ray.init(runtime_env=self.config.ray.runtime_env)
        
        self.data_access = DataAccessFactory.create('s3', region=self.config.region)
    
    def run(self):
        """Execute pipeline steps based on configuration."""
        self.initialize()
        
        try:
            # Discover files
            files = self._discover_files()
            print(f"Discovered {len(files)} files")
            
            # Execute enabled steps
            data = files
            
            if self.config.processing.normalization:
                print("Running normalization...")
                data = self._normalize_step(data)
            
            if self.config.processing.feature_engineering:
                print("Running feature engineering...")
                data = self._feature_engineering_step(data)
            
            if self.config.processing.training:
                print("Running training...")
                model = self._training_step(data)
                data = model
            
            if self.config.processing.inference:
                print("Running inference...")
                data = self._inference_step(data)
            
            if self.config.processing.backtest:
                print("Running backtest...")
                results = self._backtest_step(data)
                return results
            
            return data
            
        finally:
            self.shutdown()
    
    def _discover_files(self) -> list[tuple[str, int]]:
        """Discover files based on data config."""
        if not self.data_access:
            self.initialize()
        
        # For now, discover from raw_data_path
        # TODO: Add date range and exchange filtering
        return self.data_access.list_files(self.config.data.raw_data_path)
    
    def _normalize_step(self, files: list[tuple[str, int]]) -> Any:
        """Execute normalization step."""
        normalization = self.config.processing.normalization
        normalized_base_path = self.config.storage.normalized_path
        raw_base_path = self.config.data.raw_data_path
        
        # Create Ray remote function for normalization
        @ray.remote
        def normalize_file(file_path: str, region: str, raw_base: str, normalized_base: str) -> dict:
            import polars as pl
            from data_preprocessing.data_access.factory import DataAccessFactory
            
            data_access = DataAccessFactory.create('s3', region=region)
            
            # Read raw data
            df = data_access.read(file_path)
            
            # Normalize (for BMLL, this is pass-through)
            normalized = normalization.normalize(df, 'trades')
            
            # Construct output path maintaining same structure
            # Replace raw base path with normalized base path
            output_path = file_path.replace(raw_base, normalized_base)
            
            # Write normalized data
            data_access.write(normalized, output_path)
            
            # Get row count
            row_count = normalized.select(pl.count()).collect().item()
            
            return {
                'input_path': file_path,
                'output_path': output_path,
                'row_count': row_count
            }
        
        # Process files in parallel
        file_paths = [f[0] for f in files]
        futures = [
            normalize_file.remote(fp, self.config.region, raw_base_path, normalized_base_path) 
            for fp in file_paths
        ]
        results = ray.get(futures)
        
        print(f"Normalized {len(results)} files")
        for result in results[:5]:  # Show first 5
            print(f"  {result['input_path']} -> {result['output_path']} ({result['row_count']:,} rows)")
        
        return results
    
    def _feature_engineering_step(self, data: Any) -> Any:
        """Execute feature engineering step."""
        feature_engineering = self.config.processing.feature_engineering
        # TODO: Implement feature engineering
        print("Feature engineering not yet implemented")
        return data
    
    def _training_step(self, data: Any) -> Any:
        """Execute training step."""
        training = self.config.processing.training
        # TODO: Implement training
        print("Training not yet implemented")
        return data
    
    def _inference_step(self, data: Any) -> Any:
        """Execute inference step."""
        inference = self.config.processing.inference
        # TODO: Implement inference
        print("Inference not yet implemented")
        return data
    
    def _backtest_step(self, data: Any) -> Any:
        """Execute backtest step."""
        backtest = self.config.processing.backtest
        # TODO: Implement backtest
        print("Backtest not yet implemented")
        return data
    
    def shutdown(self):
        """Shutdown Ray."""
        if ray.is_initialized():
            ray.shutdown()
