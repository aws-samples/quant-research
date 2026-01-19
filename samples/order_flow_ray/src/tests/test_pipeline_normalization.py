"""Integration test for pipeline execution and schema validation."""
import sys
import os

current_dir = os.getcwd()
src_dir = os.path.dirname(current_dir)

sys.path.append(src_dir)

import polars as pl
from pipeline.config import PipelineConfig, DataConfig, ProcessingConfig, StorageConfig, RayConfig, S3Location
from pipeline.pipeline import Pipeline
from data_preprocessing.data_normalization import BMLLNormalizer
from data_preprocessing.data_access import DataAccessFactory


def test_pipeline_execution():
    """Test complete pipeline: create, validate paths, run, compare schemas."""
    
    # 1. Create pipeline
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
        ray=RayConfig(runtime_env={"working_dir": src_dir}, max_retries=3)
    )
    
    pipeline = Pipeline(config)
    
    # 2. Run pipeline and get results
    results = pipeline.run()
    assert len(results) > 0, "No files processed"
    
    # 3. Inspect paths from pipeline results
    result = results[0]
    input_file = result['input_path']
    output_file = result['output_path']
    
    assert input_file != output_file, "Input and output paths must differ"
    assert not output_file.startswith(config.data.raw_data_path), "Output must not be under raw path"
    
    print(f"✓ Pipeline executed and paths validated")
    print(f"  Input:  {input_file}")
    print(f"  Output: {output_file}")
    
    # 4. Read output (100 records)
    data_access = DataAccessFactory.create('s3', region='us-east-1')
    output_data = data_access.read(output_file).limit(100).collect()
    
    print(f"✓ Read {len(output_data)} records from output")
    
    # 5. Compare schemas
    normalizer = BMLLNormalizer()
    expected_schema = normalizer.get_schema('trades')
    
    missing = set(expected_schema.keys()) - set(output_data.columns)
    assert not missing, f"Missing columns: {missing}"
    
    for col, expected_type in expected_schema.items():
        actual_type = output_data.schema[col]
        assert actual_type == expected_type, f"{col}: expected {expected_type}, got {actual_type}"
    
    print(f"✓ Schema validation passed ({len(output_data.columns)} columns)")


if __name__ == "__main__":
    test_pipeline_execution()
    print("\n✓ All tests passed!")
