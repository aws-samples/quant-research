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
        normalizer = self.config.processing.normalization
        
        # Create Ray remote function for normalization
        @ray.remote
        def normalize_file(file_path: str, region: str) -> tuple[str, int]:
            import polars as pl
            from data_preprocessing.data_access.factory import DataAccessFactory
            
            data_access = DataAccessFactory.create('s3', region=region)
            df = data_access.read(file_path)
            
            # Normalize (for BMLL, this is pass-through)
            normalized = normalizer.normalize(df, 'trades')
            
            # Get row count
            row_count = normalized.select(pl.count()).collect().item()
            
            return file_path, row_count
        
        # Process files in parallel
        file_paths = [f[0] for f in files]
        futures = [normalize_file.remote(fp, self.config.region) for fp in file_paths]
        results = ray.get(futures)
        
        print(f"Normalized {len(results)} files")
        return results
    
    def _feature_engineering_step(self, data: Any) -> Any:
        """Execute feature engineering step."""
        feature_engineer = self.config.processing.feature_engineering
        # TODO: Implement feature engineering
        print("Feature engineering not yet implemented")
        return data
    
    def _training_step(self, data: Any) -> Any:
        """Execute training step."""
        trainer = self.config.processing.training
        # TODO: Implement training
        print("Training not yet implemented")
        return data
    
    def _inference_step(self, data: Any) -> Any:
        """Execute inference step."""
        predictor = self.config.processing.inference
        # TODO: Implement inference
        print("Inference not yet implemented")
        return data
    
    def _backtest_step(self, data: Any) -> Any:
        """Execute backtest step."""
        backtester = self.config.processing.backtest
        # TODO: Implement backtest
        print("Backtest not yet implemented")
        return data
    
    def shutdown(self):
        """Shutdown Ray."""
        if ray.is_initialized():
            ray.shutdown()
