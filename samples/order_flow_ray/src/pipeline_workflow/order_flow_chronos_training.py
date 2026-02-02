"""Pipeline workflow for Chronos2 model training."""
import sys
import os

# Path setup
current_dir = os.getcwd()
if 'order_flow_ray' in current_dir:
    src_dir = os.path.dirname(current_dir)
else:
    src_dir = os.path.join(current_dir, 'samples', 'order_flow_ray', 'src')
sys.path.append(src_dir)

from pipeline.config import (
    PipelineConfig, DataConfig, ProcessingConfig,
    StorageConfig, RayConfig, S3Location
)
from pipeline.pipeline import Pipeline
from data_normalization import BMLLNormalizer
from feature_engineering import (
    OrderFlowFeatureEngineer,
    TradeFeatureEngineering,
    L2QFeatureEngineering
)
from model_training import ChronosTrainer


def main():
    """Run full pipeline with Chronos2 training.

    This workflow trains a single global Chronos2 model on mid-price
    predictions for all tickers. Training may take multiple days.

    Configuration:
        - Data: Full month of BMLL order flow data (trades + L2Q)
        - Features: 1-second bars with mid-price target
        - Model: Chronos-2 (120M encoder-only) with 30-bar prediction horizon
        - Training: 50 epochs with 4 GPU workers
    """
    config = PipelineConfig(
        region='us-east-1',
        data=DataConfig(
            raw_data_path='s3://bmlldata',
            start_date='2024-01-02',
            end_date='2024-01-31',  # Full month for training
            exchanges=['XNAS', 'XNYS', 'ARCX'],
            data_types=['trades', 'level2q']  # Need both for mid-price
        ),
        processing=ProcessingConfig(
            normalization=BMLLNormalizer(),
            feature_engineering=OrderFlowFeatureEngineer(
                bar_duration_ms=1000,  # 1-second bars
                trade_feature_engineering=TradeFeatureEngineering(),
                l2q_feature_engineering=L2QFeatureEngineering()  # For mid-price
            ),
            training=ChronosTrainer(
                # Chronos-2: 120M encoder-only model with native covariate support
                # Past covariates: price_close, volume, spread, imbalances (automatic)
                prediction_length=30,  # 30 bars = 30 seconds ahead
                context_length=512,    # 512 bars = ~8.5 minutes context
                num_epochs=50,
                learning_rate=1e-4,
                batch_size=32,
                num_workers=4,
                use_gpu=True,
                checkpoint_frequency=5  # Save every 5 epochs
            )
        ),
        storage=StorageConfig(
            raw_data=S3Location(path='s3://bmlldata'),
            normalized=S3Location(
                path='s3://orderflowanalysis/intermediate/normalized'
            ),
            features=S3Location(
                path='s3://orderflowanalysis/intermediate/features'
            ),
            models=S3Location(
                path='s3://orderflowanalysis/output/models'
            ),
            predictions=S3Location(
                path='s3://orderflowanalysis/output/predictions'
            ),
            backtest=S3Location(
                path='s3://orderflowanalysis/output/backtest'
            )
        ),
        ray=RayConfig(
            runtime_env={"working_dir": src_dir},
            flat_core_count=5,
            memory_multiplier=2.0,
            memory_per_core_gb=4.0,
            max_retries=3,
            skip_runtime_env=True
        ),
        profile_name='default'
    )

    print("=" * 80)
    print("CHRONOS2 TRAINING PIPELINE")
    print("=" * 80)
    print(f"Date range: {config.data.start_date} to {config.data.end_date}")
    print(f"Exchanges: {', '.join(config.data.exchanges)}")
    print(f"Data types: {', '.join(config.data.data_types)}")
    print(f"Model: {config.processing.training.model_variant}")
    print(f"Prediction horizon: {config.processing.training.prediction_length} bars")
    print("=" * 80)

    # Run pipeline
    pipeline = Pipeline(config)
    results = pipeline.run()

    print("\n" + "=" * 80)
    print("PIPELINE COMPLETE")
    print("=" * 80)
    print(f"Training result:")
    print(f"  Status: {results.get('status')}")
    print(f"  Run ID: {results.get('training_run_id')}")
    print(f"  Checkpoint: {results.get('checkpoint_path')}")
    val_loss = results.get('val_loss')
    print(f"  Val Loss: {val_loss:.4f}" if val_loss is not None else "  Val Loss: N/A")
    print("=" * 80)


if __name__ == '__main__':
    main()
