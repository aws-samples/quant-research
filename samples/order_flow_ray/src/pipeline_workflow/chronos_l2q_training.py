"""Standalone Chronos-2 training pipeline for L2Q order book data.

Loads Level 2 Quote data from S3, engineers features, and trains
a Chronos-2 model for mid-price prediction.

Target: mid_price = (BidPrice1 + AskPrice1) / 2

Past Covariates (from order book):
- spread, spread_pct
- bid_quantity_1, ask_quantity_1, quantity_imbalance
- bid_num_orders_1, ask_num_orders_1, order_imbalance
- total_bid_depth, total_ask_depth, book_imbalance
- bid_level_count, ask_level_count

Future Covariates (time-based, known in advance):
- minutes_to_open (minutes since market open)
- minutes_to_close (minutes until market close)

Usage:
    python chronos_l2q_training.py
"""
import sys
import os
from dataclasses import dataclass
from typing import List, Optional
from datetime import datetime, time

# Path setup
current_dir = os.getcwd()
if 'pipeline_workflow' in current_dir:
    src_dir = os.path.dirname(current_dir)
elif 'order_flow_ray' in current_dir:
    src_dir = os.path.join(current_dir, 'src')
else:
    src_dir = os.path.join(current_dir, 'samples', 'order_flow_ray', 'src')
sys.path.append(src_dir)

import ray
import ray.data
import polars as pl
import numpy as np
import boto3


# =============================================================================
# Configuration
# =============================================================================

@dataclass
class L2QTrainingConfig:
    """Configuration for L2Q Chronos training pipeline."""

    # S3 Data Location
    s3_bucket: str = "bmlldata"
    s3_prefix: str = "level2q"  # Path pattern: s3://bucket/prefix/YYYY/MM/DD/...

    # Data Selection
    start_date: str = "2024-01-02"
    end_date: str = "2024-01-05"  # Start small for testing
    exchanges: List[str] = None  # e.g., ['XNAS', 'XNYS']
    tickers: Optional[List[str]] = None  # None = all tickers

    # Time Bar Aggregation
    bar_duration_ms: int = 1000  # 1-second bars

    # Market Hours (US Eastern)
    market_open_hour: int = 9
    market_open_minute: int = 30
    market_close_hour: int = 16
    market_close_minute: int = 0

    # Chronos Model Config
    model_variant: str = "amazon/chronos-2"
    prediction_length: int = 30  # 30 bars ahead
    context_length: int = 512  # ~8.5 minutes context

    # Covariate Features (configurable)
    past_covariates: List[str] = None  # Features known from historical data
    future_covariates: List[str] = None  # Features known in advance (time-based)

    # Training Config
    num_epochs: int = 10
    learning_rate: float = 1e-4
    batch_size: int = 32
    num_workers: int = 2
    use_gpu: bool = True
    checkpoint_frequency: int = 2

    # Output
    output_path: str = "s3://orderflowanalysis/models/chronos-l2q"

    # AWS
    aws_profile: str = "default"
    aws_region: str = "us-east-1"

    def __post_init__(self):
        if self.exchanges is None:
            self.exchanges = ['XNAS', 'XNYS']

        # Default past covariates (order book features)
        if self.past_covariates is None:
            self.past_covariates = [
                'spread_mean',
                'spread_pct_mean',
                'quantity_imbalance',
                'order_imbalance',
                'total_bid_depth',
                'total_ask_depth',
                'book_imbalance',
                'bid_quantity_1',
                'ask_quantity_1',
                'bid_num_orders_1',
                'ask_num_orders_1',
                'bid_level_count',
                'ask_level_count',
            ]

        # Default future covariates (time-based features)
        if self.future_covariates is None:
            self.future_covariates = [
                'minutes_to_open',
                'minutes_to_close',
            ]


# =============================================================================
# Data Loading
# =============================================================================

def get_s3_paths(config: L2QTrainingConfig) -> List[str]:
    """Generate S3 paths for date range.

    Assumes path pattern: s3://bucket/prefix/YYYY/MM/DD/exchange/*.parquet
    """
    from datetime import datetime, timedelta

    paths = []
    start = datetime.strptime(config.start_date, "%Y-%m-%d")
    end = datetime.strptime(config.end_date, "%Y-%m-%d")

    current = start
    while current <= end:
        year = current.strftime("%Y")
        month = current.strftime("%m")
        day = current.strftime("%d")

        for exchange in config.exchanges:
            path = f"s3://{config.s3_bucket}/{config.s3_prefix}/{year}/{month}/{day}/{exchange}/"
            paths.append(path)

        current += timedelta(days=1)

    return paths


def load_l2q_data(config: L2QTrainingConfig) -> ray.data.Dataset:
    """Load L2Q parquet data from S3 using Ray.

    Args:
        config: Training configuration

    Returns:
        Ray Dataset with L2Q data
    """
    print("=" * 80)
    print("LOADING L2Q DATA FROM S3")
    print("=" * 80)

    # Get S3 paths
    s3_paths = get_s3_paths(config)
    print(f"Date range: {config.start_date} to {config.end_date}")
    print(f"Exchanges: {config.exchanges}")
    print(f"S3 paths to scan: {len(s3_paths)}")

    # Get AWS credentials
    session = boto3.Session(profile_name=config.aws_profile)
    credentials = session.get_credentials()

    # Configure filesystem options for Ray
    filesystem_options = {
        "region": config.aws_region,
    }

    # Add credentials if available
    if credentials:
        filesystem_options["access_key"] = credentials.access_key
        filesystem_options["secret_key"] = credentials.secret_key
        if credentials.token:
            filesystem_options["session_token"] = credentials.token

    # Load parquet files with Ray
    # ZSTD compression is handled automatically by PyArrow
    print("\nLoading parquet files...")

    try:
        dataset = ray.data.read_parquet(
            s3_paths,
            filesystem="s3",
            arrow_open_stream_args={"compression": "zstd"},
        )

        row_count = dataset.count()
        print(f"Loaded {row_count:,} rows")

        # Filter by tickers if specified
        if config.tickers:
            print(f"Filtering to tickers: {config.tickers}")
            dataset = dataset.filter(
                lambda row: row["Ticker"] in config.tickers
            )
            filtered_count = dataset.count()
            print(f"After filtering: {filtered_count:,} rows")

        return dataset

    except Exception as e:
        print(f"Error loading data: {e}")
        print("Attempting alternative loading method...")

        # Alternative: load paths one by one
        datasets = []
        for path in s3_paths:
            try:
                ds = ray.data.read_parquet(path)
                datasets.append(ds)
            except Exception as path_error:
                print(f"  Skipping {path}: {path_error}")

        if not datasets:
            raise ValueError("No data could be loaded from any path")

        # Union all datasets
        dataset = datasets[0]
        for ds in datasets[1:]:
            dataset = dataset.union(ds)

        return dataset


# =============================================================================
# Feature Engineering
# =============================================================================

def engineer_features(dataset: ray.data.Dataset, config: L2QTrainingConfig) -> pl.DataFrame:
    """Engineer features from raw L2Q data.

    Creates:
    - Target: mid_price
    - Past covariates: spread, imbalance, depth features
    - Future covariates: minutes_to_open, minutes_to_close
    - Time bars: aggregated to bar_duration_ms

    Args:
        dataset: Ray Dataset with raw L2Q data
        config: Training configuration

    Returns:
        Polars DataFrame with engineered features
    """
    print("\n" + "=" * 80)
    print("ENGINEERING FEATURES")
    print("=" * 80)

    # Convert Ray Dataset to Polars for efficient feature engineering
    print("Converting to Polars DataFrame...")
    df = pl.from_arrow(dataset.to_arrow())
    print(f"DataFrame shape: {df.shape}")

    # Parse timestamp and calculate bar_id
    print("Calculating time bars...")
    df = df.with_columns([
        # Parse EventTimestamp to datetime
        pl.col("EventTimestamp").str.to_datetime().alias("event_dt"),
    ])

    # Calculate milliseconds since midnight for bar assignment
    df = df.with_columns([
        (
            pl.col("event_dt").dt.hour() * 3600000 +
            pl.col("event_dt").dt.minute() * 60000 +
            pl.col("event_dt").dt.second() * 1000 +
            pl.col("event_dt").dt.millisecond()
        ).alias("ms_since_midnight")
    ])

    # Calculate bar_id (rounded to bar_duration_ms)
    df = df.with_columns([
        (
            (pl.col("ms_since_midnight") / config.bar_duration_ms).ceil() * config.bar_duration_ms
        ).cast(pl.Int64).alias("bar_id")
    ])

    # Calculate target: mid_price
    print("Calculating mid_price target...")
    df = df.with_columns([
        ((pl.col("BidPrice1") + pl.col("AskPrice1")) / 2.0).alias("mid_price")
    ])

    # Calculate past covariate features (raw tick level)
    print("Calculating past covariate features...")
    df = df.with_columns([
        # Spread features
        (pl.col("AskPrice1") - pl.col("BidPrice1")).alias("spread"),
        (
            (pl.col("AskPrice1") - pl.col("BidPrice1")) /
            ((pl.col("BidPrice1") + pl.col("AskPrice1")) / 2.0) * 100
        ).alias("spread_pct"),

        # Quantity imbalance
        (
            (pl.col("BidQuantity1") - pl.col("AskQuantity1")) /
            (pl.col("BidQuantity1") + pl.col("AskQuantity1") + 1e-8)
        ).alias("quantity_imbalance"),

        # Order count imbalance
        (
            (pl.col("BidNumOrders1") - pl.col("AskNumOrders1")) /
            (pl.col("BidNumOrders1") + pl.col("AskNumOrders1") + 1e-8)
        ).alias("order_imbalance"),

        # Total depth (5 levels)
        (
            pl.col("BidQuantity1") + pl.col("BidQuantity2") +
            pl.col("BidQuantity3") + pl.col("BidQuantity4") + pl.col("BidQuantity5")
        ).alias("total_bid_depth"),
        (
            pl.col("AskQuantity1") + pl.col("AskQuantity2") +
            pl.col("AskQuantity3") + pl.col("AskQuantity4") + pl.col("AskQuantity5")
        ).alias("total_ask_depth"),
    ])

    # Book imbalance from total depth
    df = df.with_columns([
        (
            (pl.col("total_bid_depth") - pl.col("total_ask_depth")) /
            (pl.col("total_bid_depth") + pl.col("total_ask_depth") + 1e-8)
        ).alias("book_imbalance")
    ])

    # Calculate future covariates (time-based)
    print("Calculating future covariate features...")
    market_open_ms = (config.market_open_hour * 60 + config.market_open_minute) * 60000
    market_close_ms = (config.market_close_hour * 60 + config.market_close_minute) * 60000

    df = df.with_columns([
        # Minutes since market open
        ((pl.col("ms_since_midnight") - market_open_ms) / 60000.0).alias("minutes_to_open"),
        # Minutes until market close
        ((market_close_ms - pl.col("ms_since_midnight")) / 60000.0).alias("minutes_to_close"),
    ])

    # Aggregate to time bars
    print(f"Aggregating to {config.bar_duration_ms}ms bars...")

    # Group by Ticker, TradeDate, and bar_id
    df_bars = df.group_by(["Ticker", "TradeDate", "bar_id"]).agg([
        # Target: last mid_price in bar (close)
        pl.col("mid_price").last().alias("mid_price"),

        # Past covariates: mean within bar
        pl.col("spread").mean().alias("spread_mean"),
        pl.col("spread_pct").mean().alias("spread_pct_mean"),
        pl.col("quantity_imbalance").mean().alias("quantity_imbalance"),
        pl.col("order_imbalance").mean().alias("order_imbalance"),
        pl.col("total_bid_depth").mean().alias("total_bid_depth"),
        pl.col("total_ask_depth").mean().alias("total_ask_depth"),
        pl.col("book_imbalance").mean().alias("book_imbalance"),

        # Additional raw features: last value
        pl.col("BidQuantity1").last().alias("bid_quantity_1"),
        pl.col("AskQuantity1").last().alias("ask_quantity_1"),
        pl.col("BidNumOrders1").last().alias("bid_num_orders_1"),
        pl.col("AskNumOrders1").last().alias("ask_num_orders_1"),
        pl.col("BidLevelCount").last().alias("bid_level_count"),
        pl.col("AskLevelCount").last().alias("ask_level_count"),

        # Future covariates: last value (same for entire bar)
        pl.col("minutes_to_open").last().alias("minutes_to_open"),
        pl.col("minutes_to_close").last().alias("minutes_to_close"),

        # Count for verification
        pl.count().alias("tick_count"),
    ]).sort(["Ticker", "TradeDate", "bar_id"])

    print(f"Aggregated to {len(df_bars):,} bars")
    print(f"Unique tickers: {df_bars['Ticker'].n_unique()}")

    # Show sample
    print("\nSample of engineered features:")
    print(df_bars.head(5))

    return df_bars


# =============================================================================
# Sequence Creation & Training
# =============================================================================

def create_sequences_and_train(
    features_df: pl.DataFrame,
    config: L2QTrainingConfig
) -> dict:
    """Create sequences and train Chronos-2 model.

    Uses the existing chronos_training_utils for sequence creation
    and ChronosTrainer for training.

    Args:
        features_df: Engineered features DataFrame
        config: Training configuration

    Returns:
        Training results dictionary
    """
    from model_training.chronos_training_utils import (
        create_time_series_sequences,
        normalize_features,
        temporal_train_val_split
    )
    from model_training.chronos_trainer import ChronosTrainer

    print("\n" + "=" * 80)
    print("CREATING SEQUENCES AND TRAINING")
    print("=" * 80)

    # Use covariates from config
    past_covariates = config.past_covariates
    future_covariates = config.future_covariates

    print(f"Past covariates ({len(past_covariates)}): {past_covariates}")
    print(f"Future covariates ({len(future_covariates)}): {future_covariates}")

    # Rename mid_price columns for compatibility
    # (chronos_training_utils expects 'bid_price_mean' and 'ask_price_mean' for mid_price calc)
    # Since we already calculated mid_price, we can use it directly

    # Create sequences
    print("\nCreating time series sequences...")
    sequences = create_time_series_sequences(
        features_df,
        target_column='mid_price',
        context_length=config.context_length,
        prediction_length=config.prediction_length,
        past_covariates=past_covariates,
        future_covariates=future_covariates,
        group_by_column='Ticker'
    )

    if not sequences:
        raise ValueError(
            f"No sequences created. Need at least {config.context_length + config.prediction_length} "
            f"bars per ticker. Check data volume."
        )

    print(f"Created {len(sequences)} sequences")

    # Normalize
    print("\nNormalizing features...")
    sequences_norm, scaler_params = normalize_features(sequences)

    # Split
    print("\nSplitting train/val...")
    train_seq, val_seq = temporal_train_val_split(sequences_norm, split_ratio=0.8)

    # Convert to Ray Dataset
    print("\nConverting to Ray Dataset...")
    train_dataset = ray.data.from_items(train_seq)
    val_dataset = ray.data.from_items(val_seq)

    print(f"Train sequences: {train_dataset.count()}")
    print(f"Val sequences: {val_dataset.count()}")

    # Create trainer
    print("\nInitializing ChronosTrainer...")
    trainer = ChronosTrainer(
        model_variant=config.model_variant,
        prediction_length=config.prediction_length,
        context_length=config.context_length,
        past_covariates=past_covariates,
        future_covariates=future_covariates,
        num_epochs=config.num_epochs,
        learning_rate=config.learning_rate,
        batch_size=config.batch_size,
        num_workers=config.num_workers,
        use_gpu=config.use_gpu,
        checkpoint_frequency=config.checkpoint_frequency
    )

    # Prepare storage location
    storage_location = {
        'path': config.output_path,
        'access_type': 's3'
    }

    # Train (bypass _prepare_training_data since we already have datasets)
    print("\n" + "=" * 80)
    print("STARTING CHRONOS-2 TRAINING")
    print("=" * 80)
    print(f"Model: {config.model_variant}")
    print(f"Context length: {config.context_length} bars")
    print(f"Prediction length: {config.prediction_length} bars")
    print(f"Epochs: {config.num_epochs}")
    print(f"Workers: {config.num_workers}")
    print(f"GPU: {config.use_gpu}")

    # Use the internal training components directly
    from ray.train import ScalingConfig, RunConfig, CheckpointConfig
    from ray.train.torch import TorchTrainer
    from ray.air import FailureConfig

    scaling_config = ScalingConfig(
        num_workers=config.num_workers,
        use_gpu=config.use_gpu,
        resources_per_worker={"GPU": 1} if config.use_gpu else {"CPU": 4}
    )

    checkpoint_config = CheckpointConfig(
        num_to_keep=3,
        checkpoint_frequency=config.checkpoint_frequency,
        checkpoint_score_attribute="val_loss",
        checkpoint_score_order="min"
    )

    run_config = RunConfig(
        name=f"chronos2_l2q_{datetime.now().strftime('%Y%m%d_%H%M%S')}",
        storage_path=config.output_path,
        checkpoint_config=checkpoint_config,
        failure_config=FailureConfig(max_failures=3)
    )

    ray_trainer = TorchTrainer(
        train_loop_per_worker=ChronosTrainer._train_loop,
        train_loop_config={
            'model_variant': config.model_variant,
            'prediction_length': config.prediction_length,
            'context_length': config.context_length,
            'past_covariates': past_covariates,
            'future_covariates': future_covariates,
            'num_epochs': config.num_epochs,
            'learning_rate': config.learning_rate,
            'batch_size': config.batch_size,
            'train_dataset': train_dataset,
            'val_dataset': val_dataset,
            'scaler_params': scaler_params
        },
        scaling_config=scaling_config,
        run_config=run_config
    )

    result = ray_trainer.fit()

    # Extract results
    checkpoint_path = result.checkpoint.path if result.checkpoint else None
    final_metrics = result.metrics or {}

    print("\n" + "=" * 80)
    print("TRAINING COMPLETE")
    print("=" * 80)
    print(f"Checkpoint: {checkpoint_path}")
    if 'val_loss' in final_metrics:
        print(f"Final val_loss: {final_metrics['val_loss']:.4f}")

    return {
        'status': 'success',
        'checkpoint_path': checkpoint_path,
        'metrics': final_metrics,
        'scaler_params': scaler_params,
        'config': {
            'model_variant': config.model_variant,
            'prediction_length': config.prediction_length,
            'context_length': config.context_length,
            'past_covariates': past_covariates,
            'future_covariates': future_covariates,
        }
    }


# =============================================================================
# Main Entry Point
# =============================================================================

def main():
    """Run the complete L2Q Chronos training pipeline."""

    print("=" * 80)
    print("CHRONOS-2 L2Q TRAINING PIPELINE")
    print("=" * 80)
    print(f"Start time: {datetime.now().isoformat()}")

    # Configuration
    config = L2QTrainingConfig(
        # Data
        s3_bucket="bmlldata",
        s3_prefix="level2q",
        start_date="2024-01-02",
        end_date="2024-01-05",
        exchanges=['XNAS', 'XNYS'],
        tickers=None,  # All tickers

        # Time bars
        bar_duration_ms=1000,  # 1-second bars

        # Model
        model_variant="amazon/chronos-2",
        prediction_length=30,
        context_length=512,

        # Covariate features (use defaults or customize)
        # past_covariates=None uses defaults from __post_init__
        # Or specify custom list:
        # past_covariates=[
        #     'spread_mean',
        #     'quantity_imbalance',
        #     'book_imbalance',
        # ],
        # future_covariates=[
        #     'minutes_to_open',
        #     'minutes_to_close',
        # ],

        # Training
        num_epochs=10,
        learning_rate=1e-4,
        batch_size=32,
        num_workers=2,
        use_gpu=True,
        checkpoint_frequency=2,

        # Output
        output_path="s3://orderflowanalysis/models/chronos-l2q",

        # AWS
        aws_profile="default",
        aws_region="us-east-1"
    )

    # Initialize Ray
    if not ray.is_initialized():
        ray.init()

    try:
        # Step 1: Load data
        dataset = load_l2q_data(config)

        # Step 2: Engineer features
        features_df = engineer_features(dataset, config)

        # Step 3: Create sequences and train
        results = create_sequences_and_train(features_df, config)

        # Final summary
        print("\n" + "=" * 80)
        print("PIPELINE COMPLETE")
        print("=" * 80)
        print(f"Status: {results['status']}")
        print(f"Checkpoint: {results['checkpoint_path']}")
        print(f"End time: {datetime.now().isoformat()}")

        return results

    except Exception as e:
        print(f"\nPIPELINE FAILED: {e}")
        import traceback
        traceback.print_exc()
        raise

    finally:
        # Cleanup
        if ray.is_initialized():
            ray.shutdown()


if __name__ == "__main__":
    main()
