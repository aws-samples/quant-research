# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: MIT-0

"""Run feature engineering pipeline."""
import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from pipeline.config import PipelineConfig, DataConfig, ProcessingConfig, StorageConfig, RayConfig, S3Location
from pipeline.pipeline import Pipeline
from feature_engineering.order_flow import OrderFlowFeatureEngineering


def main():
    # Configuration
    BAR_DURATION_MS = 250
    
    # Build configuration
    config_recon = PipelineConfig(
        region='us-east-1',
        data=DataConfig(
            raw_data_path='s3://orderflowanalysis/intermediate/normalized',
            start_date='2024-12-31',
            end_date='2024-12-31',
            exchanges=['AMERICAS'],
            data_types=['trades', 'level2q']
        ),
        processing=ProcessingConfig(
            feature_engineering=OrderFlowFeatureEngineering(
                bar_duration_ms=BAR_DURATION_MS,
                max_section=7,
                group_filter=None
            )
        ),
        storage=StorageConfig(
            raw_data=S3Location(path='s3://orderflowanalysis/intermediate/normalized'),
            normalized=S3Location(path='s3://orderflowanalysis/intermediate/normalized'),
            repartitioned=S3Location(path='s3://orderflowanalysis/intermediate/repartitioned_v3'),
            reconciliation=S3Location(path='s3://orderflowanalysis/intermediate/reconciliation'),
            features=S3Location(path='s3://orderflowanalysis/intermediate/features'),
            models=S3Location(path='s3://orderflowanalysis/output/models'),
            predictions=S3Location(path='s3://orderflowanalysis/output/predictions'),
            backtest=S3Location(path='s3://orderflowanalysis/output/backtest'),
            metadata=S3Location(path='s3://orderflowanalysis/metadata')
        ),
        ray=RayConfig(runtime_env={}, flat_core_count=170)
    )
    
    print("Testing Feature Engineering Pipeline...")
    pipeline_recon = Pipeline(config_recon)
    
    # Run all files
    results_recon = pipeline_recon.run()  # Process all discovered files
    
    # Display results
    print("\n" + "=" * 80)
    print("Feature Engineering Pipeline Results")
    print("=" * 80)
    print(f"Total files processed: {len(results_recon)}")
    
    successful = [r for r in results_recon if r.get('message', 'success') == 'success']
    failed = [r for r in results_recon if r.get('message', 'success') != 'success']
    
    print(f"Successful: {len(successful)}")
    print(f"Failed: {len(failed)}")
    
    if successful:
        total_input_rows = sum(r.get('input_row_count', 0) for r in successful)
        total_output_rows = sum(r.get('row_count', 0) for r in successful)
        print(f"\nTotal rows: {total_input_rows:,} -> {total_output_rows:,}")
        
        print("\nSample successful files:")
        for result in successful[:5]:
            input_path = result.get('input_path', 'N/A')
            output_path = result.get('output_path', 'N/A')
            input_rows = result.get('input_row_count', 0)
            output_rows = result.get('row_count', 0)
            print(f"  {input_path}")
            print(f"    -> {output_path}")
            print(f"    ({input_rows:,} -> {output_rows:,} rows)")
    
    if failed:
        print("\nFailed files:")
        for result in failed[:5]:
            print(f"  {result.get('input_path', 'N/A')}: {result.get('message', 'Unknown error')[:100]}")


if __name__ == "__main__":
    main()
