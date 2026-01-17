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
        
        # Initialize data access for raw data
        raw_loc = self.config.storage.raw_data
        self.data_access = DataAccessFactory.create(
            raw_loc.get_access_type(),
            region=self.config.region,
            profile_name=self.config.profile_name
        )
        
        # Initialize data access for normalized output
        norm_loc = self.config.storage.normalized
        if norm_loc.get_access_type() == 's3tables':
            self.normalized_access = DataAccessFactory.create(
                's3tables',
                region=self.config.region,
                table_bucket_arn=norm_loc.table_bucket_arn,
                namespace=norm_loc.namespace,
                profile_name=self.config.profile_name
            )
            self.normalized_access.create_namespace()
        else:
            self.normalized_access = self.data_access
    
    def run(self, max_files: int | None = None, specific_files: list[str] | None = None):
        """Execute pipeline steps based on configuration.
        
        Args:
            max_files: Optional limit on number of files to process for testing
            specific_files: Optional list of specific file paths to process
        """
        self.initialize()
        
        try:
            # Discover or use specific files
            if specific_files:
                files = [(path, 0) for path in specific_files]  # Size not needed for specific files
                print(f"Processing {len(files)} specific files")
            else:
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
        
        # Filter out reference data and sort by size ascending
        files = [(path, size) for path, size in files if '/reference/' not in path]
        files.sort(key=lambda x: x[1])
        
        return files
    
    def _normalize_step(self, files: list[tuple[str, int]]) -> Any:
        """Execute normalization step."""
        normalization = self.config.processing.normalization
        normalized_loc = self.config.storage.normalized
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
            
            # Create Ray remote function with dynamic CPUs
            @ray.remote(num_cpus=num_cpus)
            def normalize_file(fp: str, fs: float, region: str, raw_base: str, norm_loc_dict: dict, mem_gb: float, cpus: int, profile: str) -> dict:
                import polars as pl
                from data_preprocessing.data_access.factory import DataAccessFactory
                
                # Read from raw
                data_access = DataAccessFactory.create('s3', region=region, profile_name=profile)
                df = data_access.read(fp)
                normalized = normalization.normalize(df, 'trades', source_path=fp)
                
                # Write to normalized location
                if norm_loc_dict['access_type'] == 's3tables':
                    output_access = DataAccessFactory.create(
                        's3tables',
                        region=region,
                        table_bucket_arn=norm_loc_dict['table_bucket_arn'],
                        namespace=norm_loc_dict['namespace'],
                        profile_name=profile
                    )
                    table_name = norm_loc_dict['table_name']
                    output_access.write(normalized, table_name, mode='append', partition_by=['Year', 'Month', 'Day', 'DataType', 'Region', 'ISOExchangeCode'])
                    output_path = f"{norm_loc_dict['namespace']}.{table_name}"
                else:
                    output_access = data_access
                    _, relative_path = fp.split(raw_base.rstrip('/') + '/', 1)
                    output_path = f"{norm_loc_dict['path'].rstrip('/')}/{relative_path}"
                    output_access.write(normalized, output_path)
                
                row_count = normalized.select(pl.len()).collect().item()
                
                return {
                    'file': fp.split('/')[-1],
                    'size_gb': fs,
                    'memory_gb': mem_gb,
                    'cpus': cpus,
                    'input_path': fp,
                    'output_path': output_path,
                    'row_count': row_count
                }
            
            # Serialize normalized location
            norm_dict = {
                'access_type': normalized_loc.get_access_type(),
                'path': normalized_loc.get_path(),
            }
            if normalized_loc.get_access_type() == 's3tables':
                norm_dict['table_bucket_arn'] = normalized_loc.table_bucket_arn
                norm_dict['namespace'] = normalized_loc.namespace
                norm_dict['table_name'] = normalized_loc.table_name
            
            futures.append(normalize_file.remote(
                file_path, file_size, self.config.region, raw_base_path, 
                norm_dict, memory_gb, num_cpus, self.config.profile_name
            ))
        
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
