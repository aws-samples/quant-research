"""Run BMLL processing pipeline."""
import os
from pipeline.pipeline import Pipeline
from pipeline.config import PipelineConfig, DataConfig, ProcessingConfig, StorageConfig, RayConfig
from data_preprocessing.data_normalization import BMLLNormalizer


def main():
    # Get source directory for Ray runtime
    src_dir = os.path.dirname(os.path.abspath(__file__))
    
    # Build configuration
    config = PipelineConfig(
        region='us-east-1',
        data=DataConfig(
            raw_data_path='s3://bmll-data-lab-sandbox-us-east-1/data/level_2/XNAS/',
            start_date='2024-01-01',
            end_date='2024-01-31',
            exchanges=['XNAS'],
            data_types=['trades', 'level2q']
        ),
        processing=ProcessingConfig(
            normalization=BMLLNormalizer(),
            feature_engineering=None,  # Skip for now
            training=None,
            inference=None,
            backtest=None
        ),
        storage=StorageConfig(
            intermediate_path='s3://bucket/intermediate',
            output_path='s3://bucket/output'
        ),
        ray=RayConfig(
            runtime_env={"working_dir": src_dir}
        )
    )
    
    # Run pipeline
    pipeline = Pipeline(config)
    results = pipeline.run()
    
    # Display results
    print("\nPipeline Results:")
    if isinstance(results, list):
        for file_path, row_count in results:
            print(f"  {file_path}: {row_count:,} rows")
        total_rows = sum(r[1] for r in results)
        print(f"\nTotal rows processed: {total_rows:,}")
    else:
        print(f"  {results}")


if __name__ == "__main__":
    main()
