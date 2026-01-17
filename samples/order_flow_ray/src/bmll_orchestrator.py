"""Run BMLL processing pipeline with orchestrator for parallel execution."""
import os
from pipeline import PipelineOrchestrator, PipelineConfig, DataConfig, ProcessingConfig, StorageConfig, RayConfig, S3Location, S3TablesLocation
from data_preprocessing.data_normalization import BMLLNormalizer


def main():
    # Get source directory for Ray runtime
    src_dir = os.path.dirname(os.path.abspath(__file__))
    
    # Build configuration
    config = PipelineConfig(
        region='us-east-1',
        data=DataConfig(
            raw_data_path='s3://bmll-data-lab-sandbox-us-east-1/data/level_2',
            start_date='2024-01-01',
            end_date='2024-01-03',  # 3 days
            exchanges=['XNAS', 'XNYS'],  # 2 exchanges
            data_types=['trades', 'level2q']
        ),
        processing=ProcessingConfig(
            normalization=BMLLNormalizer(),
            feature_engineering=None,
            training=None,
            inference=None,
            backtest=None
        ),
        storage=StorageConfig(
            raw_data=S3Location(path='s3://bmll-data-lab-sandbox-us-east-1/data/level_2'),
            normalized=S3Location(path='s3://orderflowanalysis/intermediate/normalized'),
            features=S3TablesLocation(
                table_name='features',
                table_bucket_arn='arn:aws:s3tables:us-east-1:614393260192:bucket/order-flow-analysis-s3table',
                namespace='trading'
            ),
            models=S3Location(path='s3://orderflowanalysis/output/models'),
            predictions=S3TablesLocation(
                table_name='predictions',
                table_bucket_arn='arn:aws:s3tables:us-east-1:614393260192:bucket/order-flow-analysis-s3table',
                namespace='trading'
            ),
            backtest=S3Location(path='s3://orderflowanalysis/output/backtest')
        ),
        ray=RayConfig(
            runtime_env={"working_dir": src_dir}
        )
    )
    
    # Run orchestrator (will create 6 shards: 3 dates x 2 exchanges)
    orchestrator = PipelineOrchestrator(config)
    results = orchestrator.run()
    
    # Display results
    print("\nOrchestrator Results:")
    print(f"Processed {len(results)} shards")
    for result in results:
        print(f"\nShard: {result['shard_id']}")
        print(f"  Date: {result['date']}")
        print(f"  Exchange: {result['exchange']}")
        print(f"  Result: {result['result']}")


if __name__ == "__main__":
    main()
