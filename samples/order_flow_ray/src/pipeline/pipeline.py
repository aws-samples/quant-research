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
    
    def run(self, files_slice=slice(None), specific_files: list[str] | None = None):
        """Execute pipeline steps based on configuration.
        
        Args:
            files_slice: Python slice object for file selection (default: slice(None) = all files)
                        Examples: slice(1000), slice(1000, None), slice(1500, 2300)
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
                files = files[files_slice]  # Apply slice directly
                print(f"Processing files[{files_slice}]: {len(files)} files")
            
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
        
        return self.config.processing.normalization.discover_files(
            self.data_access, 
            self.config.data.raw_data_path, 
            self.config.ray.file_sort_order
        )
    
    def _normalize_step(self, files: list[tuple[str, int]]) -> Any:
        """Execute normalization step with retry logic."""
        return self._execute_step_with_retry(
            'normalization',
            files,
            self.config.processing.normalization,
            self._run_normalization
        )
    
    def _execute_step_with_retry(self, step_name: str, data: Any, step_instance: Any, execute_func) -> Any:
        """Generic retry wrapper for pipeline steps.
        
        Args:
            step_name: Name of the step for logging
            data: Input data for the step
            step_instance: Step instance with max_retries attribute
            execute_func: Function to execute the step
            
        Returns:
            Step results
        """
        max_retries = getattr(step_instance, 'max_retries', 3)
        all_results = []
        remaining_data = data
        original_count = len(data)
        attempt_details = []
        
        for attempt in range(max_retries + 1):
            if not remaining_data:
                break
                
            if attempt > 0:
                print(f"\n{step_name.title()} retry attempt {attempt}/{max_retries} with {len(remaining_data)} items")
            
            results = execute_func(remaining_data)
            all_results.extend(results)
            
            # Count success/failure for this attempt
            attempt_successful = [r for r in results if r['message'] == 'success']
            attempt_failed = [r for r in results if r['message'] != 'success']
            
            # Log attempt results
            if attempt == 0:
                print(f"{step_name.title()} attempt {attempt + 1}: {len(attempt_successful)} success, {len(attempt_failed)} failed out of {len(results)} files")
            else:
                print(f"{step_name.title()} retry {attempt}: {len(attempt_successful)} success, {len(attempt_failed)} failed out of {len(results)} files")
            
            # Store attempt details for final summary
            attempt_details.append({
                'attempt': attempt + 1,
                'processed': len(results),
                'successful': len(attempt_successful),
                'failed': len(attempt_failed)
            })
            
            # Determine failed items using step's method
            remaining_data = step_instance.get_failed_items(results)
            
            if not remaining_data:
                break
        
        # Final reporting
        if step_name == 'normalization':
            successful = [r for r in all_results if r['message'] == 'success']
            failed = [r for r in all_results if r['message'] != 'success']
            
            print(f"\n{step_name.title()} FINAL SUMMARY:")
            print(f"Original files: {original_count}")
            print(f"Total successful: {len(successful)}/{len(all_results)}")
            print(f"Total failed: {len(failed)}/{len(all_results)}")
            
            # Show attempt breakdown
            print(f"\nAttempt breakdown:")
            for detail in attempt_details:
                if detail['attempt'] == 1:
                    print(f"  Initial attempt: {detail['successful']} success, {detail['failed']} failed")
                else:
                    print(f"  Retry {detail['attempt'] - 1}: {detail['successful']} success, {detail['failed']} failed")
            
            if failed:
                print(f"\nFailed files after {max_retries} retries:")
                for result in failed[:5]:
                    print(f"  {result['input_path']}: {result['message'][:100]}")
            
            if successful:
                print(f"\nSample successful files:")
                for result in successful[:5]:
                    input_size_mb = result['size_gb'] * 1024
                    output_size_mb = result.get('output_size_mb', 0)
                    print(f"  {result['input_path']} -> {result['output_path']} ({result['row_count']:,} rows, {input_size_mb:.1f}MB -> {output_size_mb:.1f}MB)")
        
        return all_results
    
    def _run_normalization(self, files: list[tuple[str, int]]) -> list[dict]:
        """Run normalization for given files with backpressure."""
        normalization = self.config.processing.normalization
        normalized_loc = self.config.storage.normalized
        raw_base_path = self.config.data.raw_data_path
        memory_multiplier = self.config.ray.memory_multiplier
        
        # Get max pending tasks from config (required)
        max_pending_tasks = getattr(self.config.ray, 'max_pending_tasks', None)
        if max_pending_tasks is None:
            raise ValueError("max_pending_tasks must be specified in ray configuration")
        
        # Serialize normalized location once
        norm_dict = {
            'access_type': normalized_loc.get_access_type(),
            'path': normalized_loc.get_path(),
        }
        if normalized_loc.get_access_type() == 's3tables':
            norm_dict['table_bucket_arn'] = normalized_loc.table_bucket_arn
            norm_dict['namespace'] = normalized_loc.namespace
            norm_dict['table_name'] = normalized_loc.table_name
        
        # Helper function to submit a task
        def submit_task(file_path, file_size):
            memory_bytes = int(file_size * memory_multiplier)
            memory_gb = memory_bytes / (1024 ** 3)
            num_cpus = ceil(memory_gb / self.config.ray.memory_per_core_gb) + self.config.ray.cpu_buffer
            
            @ray.remote(num_cpus=num_cpus)
            def normalize_file(fp: str, fs: float, region: str, raw_base: str, norm_loc_dict: dict, mem_gb: float, cpus: int, profile: str) -> dict:
                try:
                    import polars as pl
                    from data_preprocessing.data_access.factory import DataAccessFactory
                    
                    # Extract data_type from path: YYYY/MM/DD/{data_type}/AMERICAS/{filename}
                    parts = fp.split('/')
                    data_type = parts[-3] if len(parts) >= 3 else 'trades'
                    
                    # Read from raw
                    data_access = DataAccessFactory.create('s3', region=region, profile_name=profile)
                    df = data_access.read(fp)
                    normalized = normalization.normalize(df, data_type, source_path=fp)
                    
                    # Write to normalized location
                    if norm_loc_dict['access_type'] == 's3tables':
                        output_access = DataAccessFactory.create(
                            's3tables',
                            region=region,
                            table_bucket_arn=norm_loc_dict['table_bucket_arn'],
                            namespace=norm_loc_dict['namespace'],
                            profile_name=profile
                        )
                        # Use data_type-specific table name
                        table_name = f"{norm_loc_dict['table_name']}_{data_type}"
                        output_access.write(normalized, table_name, mode='append', partition_by=['Year', 'Month', 'Day', 'DataType', 'Region', 'ISOExchangeCode'])
                        output_path = f"{norm_loc_dict['namespace']}.{table_name}"
                    else:
                        output_access = data_access
                        _, relative_path = fp.split(raw_base.rstrip('/') + '/', 1)
                        output_path = f"{norm_loc_dict['path'].rstrip('/')}/{relative_path}"
                        output_access.write(normalized, output_path)
                    
                    row_count = normalized.select(pl.len()).collect().item()
                    
                    # Get output file size
                    if norm_loc_dict['access_type'] == 's3tables':
                        output_size_mb = 0  # S3 Tables doesn't provide file size
                    else:
                        try:
                            output_size_mb = output_access.get_file_size(output_path) / (1024 ** 2)
                        except:
                            output_size_mb = 0
                    
                    return {
                        'file': fp.split('/')[-1],
                        'size_gb': fs,
                        'memory_gb': mem_gb,
                        'cpus': cpus,
                        'input_path': fp,
                        'output_path': output_path,
                        'row_count': row_count,
                        'output_size_mb': output_size_mb,
                        'data_type': data_type,
                        'stage': str(normalization),
                        'message': 'success'
                    }
                except Exception as e:
                    return {
                        'file': fp.split('/')[-1],
                        'size_gb': fs,
                        'memory_gb': mem_gb,
                        'cpus': cpus,
                        'input_path': fp,
                        'output_path': None,
                        'row_count': None,
                        'output_size_mb': 0,
                        'data_type': parts[-3] if len(parts := fp.split('/')) >= 3 else 'unknown',
                        'stage': str(normalization),
                        'message': str(e)
                    }
            
            future = normalize_file.remote(
                file_path, file_size, self.config.region, raw_base_path,
                norm_dict, memory_gb, num_cpus, self.config.profile_name
            )
            return future
        
        # Ray backpressure pattern - exactly as documented
        pending_tasks = []
        results = []
        total_files = len(files)
        
        for file_path, file_size in files:
            # Wait if we have too many pending tasks
            if len(pending_tasks) >= max_pending_tasks:
                # Wait for at least one task to complete
                ready, pending_tasks = ray.wait(pending_tasks, num_returns=1)
                # Process completed tasks
                results.extend(ray.get(ready))
                
                # Log progress every 10 completions
                if len(results) % 10 == 0:
                    completed = len(results)
                    scheduled = len(pending_tasks)
                    remaining = total_files - completed - scheduled
                    available_cpus = ray.available_resources().get('CPU', 0)
                    total_cpus = ray.cluster_resources().get('CPU', 0)
                    print(f"Progress: {completed} completed, {scheduled} scheduled, {remaining} remaining (total: {total_files}) | CPUs: {available_cpus:.0f}/{total_cpus:.0f} available")
            
            # Submit new task
            pending_tasks.append(submit_task(file_path, file_size))
        
        # Get remaining results
        if pending_tasks:
            results.extend(ray.get(pending_tasks))
        
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
