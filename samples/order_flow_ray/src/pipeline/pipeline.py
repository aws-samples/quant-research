"""Pipeline orchestrator for Ray-based BMLL processing."""
import os
import ray
import polars as pl
from math import ceil
from typing import Any, List
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
    
    def _get_active_steps(self) -> list[tuple[str, Any]]:
        """Return list of (step_name, step_instance) for configured steps."""
        steps = []
        for step_name in ['normalization', 'repartition', 'reconciliation', 'feature_engineering', 'training', 'inference', 'backtest']:
            step = getattr(self.config.processing, step_name, None)
            if step is not None:
                steps.append((step_name, step))
        return steps
        
    def initialize(self):
        """Initialize Ray and data access."""
        if not ray.is_initialized():
            if self.config.ray.runtime_env and not self.config.ray.skip_runtime_env:
                ray.init(runtime_env=self.config.ray.runtime_env)
            else:
                ray.init()
        
        # Get first active step to determine data access type
        active_steps = self._get_active_steps()
        if not active_steps:
            raise ValueError("No processing steps configured")
        
        first_step_name, first_step = active_steps[0]
        input_location = self.config.storage.get_step_input(first_step)
        
        # Initialize data access for input location
        if input_location.get_access_type() == 's3tables':
            self.data_access = DataAccessFactory.create(
                's3tables',
                region=self.config.region,
                table_bucket_arn=input_location.table_bucket_arn,
                namespace=input_location.namespace,
                profile_name=self.config.profile_name
            )
        else:
            self.data_access = DataAccessFactory.create(
                's3',
                region=self.config.region,
                profile_name=self.config.profile_name
            )
        
        # Initialize data access for normalized output (if normalization is enabled)
        if self.config.processing.normalization:
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
        else:
            self.normalized_access = self.data_access
    
    def run(self, files_slice=slice(None), specific_files: list[str] | None = None, specific_date_types: list[tuple[str, str]] | None = None):
        """Execute pipeline steps based on configuration.
        
        Args:
            files_slice: Python slice object for file selection (default: slice(None) = all files)
                        Examples: slice(1000), slice(1000, None), slice(1500, 2300)
                        For reconciliation: applies to date/type pairs
            specific_files: Optional list of specific file paths to process (for normalization/repartition)
            specific_date_types: Optional list of (date, data_type) tuples for reconciliation
                                Examples: [('2024-12-31', 'trades'), ('2024-12-31', 'level2q')]
        """
        self.initialize()
        try:
            data = None
            
            if self.config.processing.normalization:
                print("Running normalization...")
                input_path = self.config.storage.get_step_input(self.config.processing.normalization).get_path()
                discovered_files = self.config.processing.normalization.discover_files(
                    self.data_access, input_path, self.config.ray.file_sort_order
                )
                filtered_files = self._apply_filtering(discovered_files, files_slice, specific_files)
                data = self._normalize_step(filtered_files)
            
            if self.config.processing.repartition:
                print("Running repartition...")
                input_path = self.config.storage.get_step_input(self.config.processing.repartition).get_path()
                discovered_files = self.config.processing.repartition.discover_files(
                    self.data_access, input_path, self.config.ray.file_sort_order
                )
                filtered_files = self._apply_filtering(discovered_files, files_slice, specific_files)
                file_groups = self.data_access.group_files_by_size(filtered_files)
                data = self._repartition_step(file_groups)
            
            if self.config.processing.reconciliation:
                print("Running reconciliation...")
                input_path = self.config.storage.get_step_input(self.config.processing.reconciliation).get_path()
                discovered_pairs = self.config.processing.reconciliation.discover_files(
                    self.data_access, input_path, self.config.ray.file_sort_order
                )
                filtered_pairs = self._apply_filtering(discovered_pairs, files_slice, specific_date_types)
                data = self._reconciliation_step(filtered_pairs)
            
            if self.config.processing.feature_engineering:
                print("Running feature engineering...")
                input_path = self.config.storage.get_step_input(self.config.processing.feature_engineering).get_path()
                discovered_files = self.config.processing.feature_engineering.discover_files(
                    self.data_access, input_path, self.config.ray.file_sort_order, 'asynch'
                )
                print(f"Discovered {len(discovered_files)} files from {input_path}")
                filtered_files = self._apply_filtering(discovered_files, files_slice, specific_files)
                print(f"After filtering: {len(filtered_files)} files selected for processing")
                grouped_files = self.config.processing.feature_engineering.group_files_for_processing(filtered_files)
                # Apply group filtering if configured
                if hasattr(self.config.processing.feature_engineering, 'group_filter') and self.config.processing.feature_engineering.group_filter:
                    original_count = len(grouped_files)
                    grouped_files = self.config.processing.feature_engineering.filter_groups_by_formulas(grouped_files, self.config.processing.feature_engineering.group_filter)
                    print(f"Group filtering applied: {original_count} -> {len(grouped_files)} groups")
                # Sort groups by file count in ascending order
                grouped_files.sort(key=len)
                avg_files_per_group = len(filtered_files) / len(grouped_files) if grouped_files else 0
                total_size_gb = sum(size for _, size in filtered_files)
                avg_size_per_group_gb = total_size_gb / len(grouped_files) if grouped_files else 0
                print(f"Grouped into {len(grouped_files)} file groups (avg {avg_files_per_group:.1f} files/group, avg {avg_size_per_group_gb:.2f} GB/group)")
                print(f"\nStarting feature engineering across {len(grouped_files)} groups...")
                data = self._feature_engineering_step(grouped_files)
            
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
    
    def _apply_filtering(self, discovered_data, files_slice, specific_items):
        """Apply filtering logic to discovered data."""
        if specific_items:
            specific_set = set(specific_items)
            return [item for item in discovered_data if item[0] in specific_set or item in specific_set]
        elif files_slice is not None:
            return discovered_data[files_slice]
        else:
            return discovered_data
    

    
    def _normalize_step(self, files: list[tuple[str, int]]) -> Any:
        """Execute normalization step with retry logic."""
        return self._execute_step_with_retry(
            'normalization',
            files,
            self.config.processing.normalization,
            self._run_normalization
        )
    
    def _repartition_step(self, files: list[tuple[str, int]]) -> Any:
        """Execute repartition step with retry logic."""
        print(f"Processing {len(files)} file groups for repartition")
        
        return self._execute_step_with_retry(
            'repartition',
            files,
            self.config.processing.repartition,
            self._run_repartition
        )
    
    def _reconciliation_step(self, date_type_pairs: list[tuple[str, str]]) -> Any:
        """Execute reconciliation step."""
        # Run reconciliation with pre-filtered date/type pairs
        results = self._run_reconciliation(date_type_pairs)
        
        # Aggregate mismatches and write to S3
        self._write_reconciliation_report(results)
        
        return results
    
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
                print(f"  {result.get('input_path', 'unknown')}: {result['message'][:100]}")
        
        if successful:
            print(f"\nSample successful files:")
            for result in successful[:5]:
                input_path = result.get('input_path', 'unknown')
                output_path = result.get('output_path', 'unknown')
                row_count = result.get('row_count', 0)
                input_row_count = result.get('input_row_count')
                if step_name == 'normalization':
                    input_size_mb = result.get('size_gb', 0) * 1024
                    output_size_mb = result.get('output_size_mb', 0)
                    print(f"  {input_path} -> {output_path} ({row_count:,} rows, {input_size_mb:.1f}MB -> {output_size_mb:.1f}MB)")
                elif step_name == 'feature_engineering' and input_row_count is not None:
                    print(f"  {input_path} -> {output_path} ({input_row_count:,} -> {row_count:,} rows)")
                else:
                    print(f"  {input_path} -> {output_path} ({row_count:,} rows)")
        
        return all_results
    
    def _run_normalization(self, files: list[tuple[str, int]]) -> list[dict]:
        """Run normalization for given files."""
        normalization_processor = self.config.processing.normalization
        normalization_output_loc = self.config.storage.get_step_output(normalization_processor)
        normalization_input_path = self.config.storage.get_step_input(normalization_processor).get_path()
        memory_multiplier = self.config.ray.memory_multiplier
        
        # Serialize normalization output location once
        norm_dict = {
            'access_type': normalization_output_loc.get_access_type(),
            'path': normalization_output_loc.get_path(),
        }
        if normalization_output_loc.get_access_type() == 's3tables':
            norm_dict['table_bucket_arn'] = normalization_output_loc.table_bucket_arn
            norm_dict['namespace'] = normalization_output_loc.namespace
            norm_dict['table_name'] = normalization_output_loc.table_name
        
        # Helper function to submit a task
        def submit_task(file_path, file_size):
            if self.config.ray.flat_core_count is not None:
                num_cpus = self.config.ray.flat_core_count
                memory_gb = num_cpus * self.config.ray.memory_per_core_gb
            else:
                memory_bytes = int(file_size * memory_multiplier)
                memory_gb = memory_bytes / (1024 ** 3)
                num_cpus = ceil(memory_gb / self.config.ray.memory_per_core_gb) + self.config.ray.cpu_buffer
            
            @ray.remote(num_cpus=num_cpus, max_retries=0)
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
                    normalized = normalization_processor.normalize(df, data_type, source_path=fp)
                    
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
                        'stage': str(normalization_processor),
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
                        'stage': str(normalization_processor),
                        'message': str(e)
                    }
            
            future = normalize_file.remote(
                file_path, file_size, self.config.region, normalization_input_path,
                norm_dict, memory_gb, num_cpus, self.config.profile_name
            )
            return future
        
        # Submit all tasks and wait for completion
        futures = [submit_task(file_path, file_size) for file_path, file_size in files]
        results = ray.get(futures)
        
        return results
    
    def _run_repartition(self, file_groups: list[list[tuple[str, int]]]) -> list[dict]:
        """Run repartition for given file groups."""
        repartition_processor = self.config.processing.repartition
        repartition_output_loc = self.config.storage.get_step_output(repartition_processor)
        repartition_input_path = self.config.storage.get_step_input(repartition_processor).get_path()
        memory_multiplier = self.config.ray.memory_multiplier
        
        # Serialize repartition output location once
        repart_dict = {
            'access_type': repartition_output_loc.get_access_type(),
            'path': repartition_output_loc.get_path(),
        }
        
        # Helper function to submit a task
        def submit_task(file_group):
            group_size = sum(size for _, size in file_group)
            if self.config.ray.flat_core_count is not None:
                num_cpus = self.config.ray.flat_core_count
                memory_gb = num_cpus * self.config.ray.memory_per_core_gb
            else:
                memory_bytes = int(group_size * memory_multiplier)
                memory_gb = memory_bytes / (1024 ** 3)
                num_cpus = ceil(memory_gb / self.config.ray.memory_per_core_gb) + self.config.ray.cpu_buffer
            
            @ray.remote(num_cpus=num_cpus, max_retries=0)
            def repartition_file_group(file_group: list[tuple[str, float]], region: str, input_base: str, repart_loc_dict: dict, max_retries: int, log_interval: int, mem_gb: float, cpus: int, profile: str) -> list[dict]:
                results = []
                for fp, fs in file_group:
                    try:
                        import polars as pl
                        from data_preprocessing.data_access.factory import DataAccessFactory
                        from data_preprocessing.repartition import Repartition
                        
                        # Extract data_type from path
                        parts = fp.split('/')
                        data_type = parts[-3] if len(parts) >= 3 else 'trades'
                        
                        # Read input data as LazyFrame
                        data_access = DataAccessFactory.create('s3', region=region, profile_name=profile)
                        df = data_access.read(fp)
                        
                        # Build output path base (without partition key)
                        _, relative_path = fp.split(input_base.rstrip('/') + '/', 1)
                        path_parts = relative_path.split('/')
                        output_path_base = f"{repart_loc_dict['path'].rstrip('/')}/{'/'.join(path_parts[:-1])}"
                        
                        # Repartition and write - returns list of result dicts
                        repart = Repartition(max_retries=max_retries, log_interval=log_interval)
                        partition_results = repart.repartition(df, fp, data_access, output_path_base)
                        
                        # Add common fields to each result
                        for result in partition_results:
                            result.update({
                                'file': fp.split('/')[-1],
                                'size_gb': fs,
                                'memory_gb': mem_gb,
                                'cpus': cpus,
                                'input_path': fp,
                                'data_type': data_type,
                                'stage': 'repartition',
                                'message': 'success'
                            })
                            results.append(result)
                        
                    except Exception as e:
                        results.append({
                            'file': fp.split('/')[-1],
                            'size_gb': fs,
                            'memory_gb': mem_gb,
                            'cpus': cpus,
                            'input_path': fp,
                            'output_path': None,
                            'row_count': None,
                            'output_size_mb': 0,
                            'data_type': parts[-3] if len(parts := fp.split('/')) >= 3 else 'unknown',
                            'partition_value': None,
                            'stage': 'repartition',
                            'message': str(e)
                        })
                
                return results
            
            future = repartition_file_group.remote(
                file_group, self.config.region, repartition_input_path,
                repart_dict, repartition_processor.max_retries, repartition_processor.log_interval, memory_gb, num_cpus, self.config.profile_name
            )
            return future
        
        # Submit all tasks and wait for completion
        futures = [submit_task(file_group) for file_group in file_groups]
        group_results = ray.get(futures)
        
        # Flatten results
        results = []
        for group_result in group_results:
            if isinstance(group_result, list):
                results.extend(group_result)
            else:
                results.append(group_result)
        
        return results
    
    def _run_feature_engineering(self, file_groups: List[List[tuple[str, int]]]) -> list[dict]:
        """Run feature engineering for grouped files."""
        feature_engineering_processor = self.config.processing.feature_engineering
        feature_engineering_output_loc = self.config.storage.get_step_output(feature_engineering_processor)
        feature_engineering_input_path = self.config.storage.get_step_input(feature_engineering_processor).get_path()
        memory_multiplier = self.config.ray.memory_multiplier
        
        # Serialize feature engineering output location once
        features_dict = {
            'access_type': feature_engineering_output_loc.get_access_type(),
            'path': feature_engineering_output_loc.get_path(),
        }
        if feature_engineering_output_loc.get_access_type() == 's3tables':
            features_dict['table_bucket_arn'] = feature_engineering_output_loc.table_bucket_arn
            features_dict['namespace'] = feature_engineering_output_loc.namespace
            features_dict['table_name'] = feature_engineering_output_loc.table_name
        
        # Helper function to submit a task for a file group
        def submit_task(file_group):
            if self.config.ray.flat_core_count is not None:
                num_cpus = self.config.ray.flat_core_count
                memory_gb = num_cpus * self.config.ray.memory_per_core_gb
            else:
                # Use largest file size in the group
                max_file_size = max(size for _, size in file_group)
                memory_bytes = int(max_file_size * memory_multiplier)
                memory_gb = memory_bytes / (1024 ** 3)
                num_cpus = ceil(memory_gb / self.config.ray.memory_per_core_gb) + self.config.ray.cpu_buffer
            
            #@ray.remote(num_cpus=num_cpus, max_retries=0)
            def feature_engineering_group(file_group: List[tuple[str, int]], region: str, fe_base: str, features_loc_dict: dict, mem_gb: float, cpus: int, profile: str, bar_duration_ms: int) -> List[dict]:
                print(f"Feature engineering group starting with {cpus} CPUs, {mem_gb:.1f}GB memory for {len(file_group)} files")
                results = []
                
                for file_path, file_size in file_group:
                    try:
                        import polars as pl
                        from data_preprocessing.data_access.factory import DataAccessFactory
                        from feature_engineering.order_flow import OrderFlowFeatureEngineering
                        
                        # Extract data_type from path
                        data_type = 'level2q' if 'level2q' in file_path else 'trades'
                        
                        # Read from normalized
                        data_access = DataAccessFactory.create('s3', region=region, profile_name=profile)
                        df = data_access.read(file_path)
                        input_row_count = df.select(pl.len()).collect().item()
                        
                        # Apply feature engineering
                        feature_eng = OrderFlowFeatureEngineering(bar_duration_ms=bar_duration_ms)
                        features = feature_eng.feature_computation(df, data_type)
                        
                        # Write to features location
                        if features_loc_dict['access_type'] == 's3tables':
                            output_access = DataAccessFactory.create(
                                's3tables',
                                region=region,
                                table_bucket_arn=features_loc_dict['table_bucket_arn'],
                                namespace=features_loc_dict['namespace'],
                                profile_name=profile
                            )
                            table_name = f"{features_loc_dict['table_name']}_{data_type}"
                            output_access.write(features, table_name, mode='append')
                            output_path = f"{features_loc_dict['namespace']}.{table_name}"
                        else:
                            output_access = data_access
                            _, relative_path = file_path.split(fe_base.rstrip('/') + '/', 1)
                            output_path = f"{features_loc_dict['path'].rstrip('/')}/{relative_path}"
                            output_path = output_path.replace('.parquet', f'_features_{bar_duration_ms}ms.parquet')
                            output_access.write(features, output_path)
                        
                        row_count = features.select(pl.len()).collect().item()
                        
                        # Get output file size
                        if features_loc_dict['access_type'] == 's3tables':
                            output_size_mb = 0
                        else:
                            try:
                                output_size_mb = output_access.get_file_size(output_path) / (1024 ** 2)
                            except:
                                output_size_mb = 0
                        
                        results.append({
                            'file': file_path.split('/')[-1],
                            'size_gb': file_size / (1024 ** 3),
                            'memory_gb': mem_gb,
                            'cpus': cpus,
                            'input_path': file_path,
                            'input_row_count': input_row_count,
                            'output_path': output_path,
                            'row_count': row_count,
                            'output_size_mb': output_size_mb,
                            'data_type': data_type,
                            'stage': str(feature_engineering_processor),
                            'message': 'success'
                        })
                        
                    except Exception as e:
                        results.append({
                            'file': file_path.split('/')[-1],
                            'size_gb': file_size / (1024 ** 3),
                            'memory_gb': mem_gb,
                            'cpus': cpus,
                            'input_path': file_path,
                            'output_path': None,
                            'row_count': None,
                            'output_size_mb': 0,
                            'data_type': data_type if 'data_type' in locals() else 'unknown',
                            'stage': str(feature_engineering_processor),
                            'message': str(e)
                        })
                
                return results
            
            feature_engineering_group_remote = ray.remote(num_cpus=num_cpus, max_retries=0)(feature_engineering_group)
            future = feature_engineering_group_remote.remote(
                file_group, self.config.region, feature_engineering_input_path,
                features_dict, memory_gb, num_cpus, self.config.profile_name,
                feature_engineering_processor.bar_duration_ms
            )
            return future
        
        # Submit all group tasks and wait for completion
        futures = [submit_task(file_group) for file_group in file_groups]
        group_results = ray.get(futures)
        
        # Flatten results from all groups
        results = []
        for group_result in group_results:
            results.extend(group_result)
        
        return results
    

    
    def _feature_engineering_step(self, grouped_files: list[list[tuple[str, int]]]) -> Any:
        """Execute feature engineering step with retry logic."""
        return self._execute_step_with_retry(
            'feature_engineering',
            grouped_files,
            self.config.processing.feature_engineering,
            self._run_feature_engineering
        )
    
    def _training_step(self, data: Any) -> Any:
        """Execute training step."""
        training = self.config.processing.training
        if not training:
            return data

        print("\n" + "=" * 80)
        print("TRAINING STEP")
        print("=" * 80)

        # Convert feature engineering results to LazyFrame if needed
        if isinstance(data, list):
            # Data is list of file result dicts from feature engineering
            features = self._load_feature_files(data)
        else:
            # Data is already a LazyFrame
            features = data

        # Get model storage location
        model_storage = self.config.storage.models
        storage_dict = {
            'access_type': model_storage.get_access_type(),
            'path': model_storage.get_path()
        }

        if model_storage.get_access_type() == 's3tables':
            storage_dict['table_bucket_arn'] = model_storage.table_bucket_arn
            storage_dict['namespace'] = model_storage.namespace

        # Train model (may take days)
        training_result = training.train(features, storage_dict)

        # Return training result for downstream steps (inference)
        return training_result

    def _load_feature_files(self, file_results: list) -> pl.LazyFrame:
        """Load feature files from list of file results.

        Args:
            file_results: List of dicts with 'path' keys

        Returns:
            Combined LazyFrame of all features
        """
        import boto3

        # Get storage options for S3
        session = boto3.Session(profile_name=self.config.profile_name)
        credentials = session.get_credentials()

        storage_options = {
            "aws_region": self.config.region,
            "aws_access_key_id": credentials.access_key,
            "aws_secret_access_key": credentials.secret_key
        }

        if credentials.token:
            storage_options["aws_session_token"] = credentials.token

        # Load all feature files
        paths = [result['path'] for result in file_results]
        return pl.scan_parquet(paths, storage_options=storage_options)
    
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
    
    def _run_reconciliation(self, date_type_pairs: list[tuple[str, str]]) -> list[dict]:
        """Run reconciliation for given date/type pairs."""
        reconciliation_processor = self.config.processing.reconciliation
        reconciliation_input_path = self.config.storage.get_step_input(reconciliation_processor).get_path()
        reconciliation_output_path = self.config.storage.get_step_output(reconciliation_processor).get_path()
        
        @ray.remote(num_cpus=1, max_retries=0)
        def reconcile_date_type(date: str, data_type: str, region: str, norm_path: str, repart_path: str, profile: str) -> dict:
            try:
                from data_preprocessing.data_access.factory import DataAccessFactory
                from data_preprocessing.reconciliation import Reconciliation
                
                data_access = DataAccessFactory.create('s3', region=region, profile_name=profile)
                recon = Reconciliation()
                result = recon.reconcile(date, data_type, data_access, norm_path, repart_path)
                return result
            except Exception as e:
                return {
                    'date': date,
                    'data_type': data_type,
                    'total_groups': 0,
                    'matched_groups': 0,
                    'mismatched_groups': 0,
                    'mismatches': None,
                    'message': str(e)
                }
        
        futures = [
            reconcile_date_type.remote(date, data_type, self.config.region, reconciliation_input_path, reconciliation_output_path, self.config.profile_name)
            for date, data_type in date_type_pairs
        ]
        results = ray.get(futures)
        
        return results
    
    def _write_reconciliation_report(self, results: list[dict]):
        """Write reconciliation report to S3."""
        from datetime import datetime
        import polars as pl
        
        # Generate timestamp for filename
        run_timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        
        # Collect all mismatches
        all_mismatches = []
        total_matched = 0
        total_mismatched = 0
        
        for result in results:
            if result.get('message') == 'success':
                total_matched += result['matched_groups']
                total_mismatched += result['mismatched_groups']
                
                if result['mismatched_groups'] > 0 and result['mismatches'] is not None:
                    all_mismatches.append(result['mismatches'])
        
        print(f"\n[RECONCILIATION] Summary:")
        print(f"  Total matched groups: {total_matched:,}")
        print(f"  Total mismatched groups: {total_mismatched:,}")
        
        # Write report if there are mismatches
        if all_mismatches:
            combined_mismatches = pl.concat(all_mismatches)
            
            reconciliation_path = self.config.storage.reconciliation.get_path()
            output_path = f"{reconciliation_path.rstrip('/')}/norm_to_repartition_recon_{run_timestamp}.csv"
            
            # Write to S3
            self.data_access.write_csv(combined_mismatches, output_path)
            print(f"  Reconciliation report written to: {output_path}")
        else:
            print(f"  No mismatches found - no report generated")
    
    def shutdown(self):
        """Shutdown Ray."""
        if ray.is_initialized():
            ray.shutdown()
