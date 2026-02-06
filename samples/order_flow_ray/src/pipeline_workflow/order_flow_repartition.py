"""Run data repartition pipeline."""
import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from pipeline.config import PipelineConfig, DataConfig, ProcessingConfig, StorageConfig, RayConfig, S3Location
from pipeline.pipeline import Pipeline
from data_preprocessing.repartition import Repartition


def main():
    # Get source directory for Ray runtime
    src_dir = os.path.dirname(os.path.abspath(__file__))
    
    # Build configuration
    config = PipelineConfig(
        region='us-east-1',
        data=DataConfig(
            raw_data_path='s3://bmlldata',
            start_date='2024-01-02',
            end_date='2024-01-02',
            exchanges=['ARCX'],
            data_types=['trades', 'level2q']
        ),
        processing=ProcessingConfig(
            repartition=Repartition(partition_column='Ticker', max_retries=3, log_interval=1000)
        ),
        storage=StorageConfig(
            raw_data=S3Location(path='s3://bmlldata'),
            normalized=S3Location(path='s3://orderflowanalysis/intermediate/normalized'),
            repartitioned=S3Location(path='s3://orderflowanalysis/intermediate/repartitioned'),
            features=S3Location(path='s3://orderflowanalysis/intermediate/features'),
            models=S3Location(path='s3://orderflowanalysis/output/models'),
            predictions=S3Location(path='s3://orderflowanalysis/output/predictions'),
            backtest=S3Location(path='s3://orderflowanalysis/output/backtest')
        ),
        ray=RayConfig(
            runtime_env={"working_dir": src_dir},
            flat_core_count=5,
            memory_multiplier=2.0,
            memory_per_core_gb=4.0,
            max_retries=5,
            cpu_buffer=1,
            file_sort_order="desc",
            pending_tasks_cpu_multiplier=1.1,
            skip_runtime_env=True
        )
    )
    
    # Run pipeline
    pipeline = Pipeline(config)
    results = pipeline.run()
    
    # Display results
    print("\nRepartition Pipeline Results:")
    print(f"Processed {len(results)} partitions")
    
    successful = [r for r in results if r.get('message', 'success') == 'success']
    failed = [r for r in results if r.get('message', 'success') != 'success']
    
    print(f"Successful: {len(successful)}")
    print(f"Failed: {len(failed)}")
    
    if successful:
        print("\nSample successful partitions:")
        for result in successful[:5]:
            partition_value = result.get('partition_value', 'N/A')
            row_count = result.get('row_count', 0)
            output_path = result.get('output_path', 'N/A')
            print(f"  [{partition_value}]: {row_count:,} rows -> {output_path}")
    
    if failed:
        print("\nFailed partitions:")
        for result in failed[:5]:
            print(f"  {result.get('input_path', 'N/A')}: {result.get('message', 'Unknown error')[:100]}")


if __name__ == "__main__":
    main()
