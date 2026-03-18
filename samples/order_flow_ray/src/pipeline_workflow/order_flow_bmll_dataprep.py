# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: MIT-0

"""Run BMLL processing pipeline."""
import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from pipeline.config import PipelineConfig, DataConfig, ProcessingConfig, StorageConfig, RayConfig, S3Location
from pipeline.pipeline import Pipeline
from data_preprocessing.data_normalization import BMLLNormalizer


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
            data_types=['trades']
        ),
        processing=ProcessingConfig(
            normalization=BMLLNormalizer()
        ),
        storage=StorageConfig(
            raw_data=S3Location(path='s3://bmlldata'),
            normalized=S3Location(path='s3://orderflowanalysis/intermediate/normalized'),
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
    print("\nPipeline Results:")
    print(f"Processed {len(results)} files")
    
    successful = [r for r in results if r['message'] == 'success']
    failed = [r for r in results if r['message'] != 'success']
    
    print(f"Successful: {len(successful)}")
    print(f"Failed: {len(failed)}")
    
    if successful:
        print("\nSample successful files:")
        for result in successful[:5]:
            print(f"  {result['input_path']} -> {result['output_path']} ({result['row_count']:,} rows)")
    
    if failed:
        print("\nFailed files:")
        for result in failed[:5]:
            print(f"  {result['input_path']}: {result['message'][:100]}")


if __name__ == "__main__":
    main()
