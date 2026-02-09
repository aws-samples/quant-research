"""Run data reconciliation pipeline."""
import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from pipeline.config import PipelineConfig, DataConfig, ProcessingConfig, StorageConfig, RayConfig, S3Location
from pipeline.pipeline import Pipeline
from data_preprocessing.reconciliation import Reconciliation


def main():
    import argparse
    parser = argparse.ArgumentParser(description='Run reconciliation pipeline')
    parser.add_argument('--specific-date-types', nargs='*', help='Specific date/type pairs as date1,type1 date2,type2')
    parser.add_argument('--files-slice', type=int, help='Number of files to process (slice)')
    args = parser.parse_args()
    
    # Parse specific date types
    specific_date_types = None
    if args.specific_date_types:
        specific_date_types = []
        for dt in args.specific_date_types:
            date, dtype = dt.split(',')
            specific_date_types.append((date, dtype))
    
    # Parse files slice
    files_slice = slice(args.files_slice) if args.files_slice else None
    # Build configuration
    config = PipelineConfig(
        region='us-east-1',
        data=DataConfig(
            raw_data_path='s3://orderflowanalysis/intermediate/normalized',
            start_date='2024-12-31',
            end_date='2024-12-31',
            exchanges=['AMERICAS'],
            data_types=['trades', 'level2q']
        ),
        processing=ProcessingConfig(
            reconciliation=Reconciliation(max_retries=3)
        ),
        storage=StorageConfig(
            raw_data=S3Location(path='s3://orderflowanalysis/intermediate/normalized'),
            normalized=S3Location(path='s3://orderflowanalysis/intermediate/normalized'),
            repartitioned=S3Location(path='s3://orderflowanalysis/intermediate/repartitioned_v2'),
            reconciliation=S3Location(path='s3://orderflowanalysis/intermediate/reconciliation'),
            features=S3Location(path='s3://orderflowanalysis/intermediate/features'),
            models=S3Location(path='s3://orderflowanalysis/output/models'),
            predictions=S3Location(path='s3://orderflowanalysis/output/predictions'),
            backtest=S3Location(path='s3://orderflowanalysis/output/backtest')
        ),
        ray=RayConfig(runtime_env={}, flat_core_count=1)
    )
    
    # Run pipeline
    pipeline = Pipeline(config)
    results = pipeline.run(specific_date_types=specific_date_types, files_slice=files_slice)
    
    # Display results
    print("\nReconciliation Pipeline Results:")
    print(f"Processed {len(results)} date/type combinations")
    
    successful = [r for r in results if r.get('message') == 'success']
    failed = [r for r in results if r.get('message') != 'success']
    
    print(f"Successful: {len(successful)}")
    print(f"Failed: {len(failed)}")
    
    if successful:
        total_matched = sum(r['matched_groups'] for r in successful)
        total_mismatched = sum(r['mismatched_groups'] for r in successful)
        print(f"\nTotal matched groups: {total_matched:,}")
        print(f"Total mismatched groups: {total_mismatched:,}")
        
        print("\nSample results:")
        for result in successful[:5]:
            date = result.get('date', 'N/A')
            data_type = result.get('data_type', 'N/A')
            matched = result.get('matched_groups', 0)
            mismatched = result.get('mismatched_groups', 0)
            print(f"  {date} / {data_type}: {matched:,} matched, {mismatched:,} mismatched")
    
    if failed:
        print("\nFailed reconciliations:")
        for result in failed[:5]:
            date = result.get('date', 'N/A')
            data_type = result.get('data_type', 'N/A')
            message = result.get('message', 'Unknown error')
            print(f"  {date} / {data_type}: {message[:100]}")


if __name__ == "__main__":
    main()
