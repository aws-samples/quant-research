"""Pipeline orchestrator for Ray-based BMLL processing."""
import ray
import polars as pl
from math import ceil
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
    
    def run(self, max_files: int | None = None):
        """Execute pipeline steps based on configuration.
        
        Args:
            max_files: Optional limit on number of files to process for testing
        """
        self.initialize()
        
        try:
            # Discover files
            files = self._discover_files()
            if max_files:
                files = files[:max_files]
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
        
        files = self.data_access.list_files(self.config.data.raw_data_path)
        
        # Filter out reference data and sort by size descending
        files = [(path, size) for path, size in files if '/reference/' not in path]
        files.sort(key=lambda x: x[1], reverse=True)
        
        return files
    
    def _normalize_step(self, files: list[tuple[str, int]]) -> Any:
        """Execute normalization step."""
        normalization = self.config.processing.normalization
        normalized_base_path = self.config.storage.normalized_path
        raw_base_path = self.config.data.raw_data_path
        memory_multiplier = self.config.ray.memory_multiplier
        
        # Process files in parallel
        file_paths = [f[0] for f in files]
        file_sizes = [f[1] for f in files]
        
        futures = []
        for file_path, file_size in zip(file_paths, file_sizes):
            # Calculate memory requirement and cores
            memory_bytes = int(file_size * memory_multiplier)
            memory_gb = memory_bytes / (1024 ** 3)
            num_cpus = ceil(memory_gb / self.config.ray.memory_per_core_gb) + 1
            
            # Create Ray remote function with dynamic memory and CPUs
            @ray.remote(memory=memory_bytes, num_cpus=num_cpus)
            def normalize_file(fp: str, region: str, raw_base: str, normalized_base: str) -> dict:
                import polars as pl
                from data_preprocessing.data_access.factory import DataAccessFactory
                
                data_access = DataAccessFactory.create('s3', region=region)
                df = data_access.read(fp)
                normalized = normalization.normalize(df, 'trades')
                output_path = fp.replace(raw_base, normalized_base)
                data_access.write(normalized, output_path)
                row_count = normalized.select(pl.count()).collect().item()
                
                return {
                    'input_path': fp,
                    'output_path': output_path,
                    'row_count': row_count
                }
            
            futures.append(normalize_file.remote(file_path, self.config.region, raw_base_path, normalized_base_path))
        
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
