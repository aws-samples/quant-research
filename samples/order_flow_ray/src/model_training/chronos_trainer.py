# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: MIT-0

"""Chronos2 time series forecasting trainer with full pretraining."""
from typing import Any, List, Dict, Optional
import polars as pl
import torch
from pathlib import Path
from datetime import datetime
import json

from ray.train import ScalingConfig, RunConfig, CheckpointConfig
from ray.train.torch import TorchTrainer
from ray.air import FailureConfig

from .base import Trainer


class ChronosTrainer(Trainer):
    """Chronos2 time series forecasting trainer with covariate support.

    Trains a single global Chronos2 model across all tickers using
    distributed data parallelism via Ray Train. Predicts mid-price
    for medium-term horizons (20-50 bars) using past covariates.

    Architecture:
        - Single global model (not per-ticker)
        - Full pretraining (multi-day training)
        - Mid-price target from L2Q orderbook data
        - Past covariates: volume, spread, imbalances, OHLC
        - Future covariates: time-based features (optional)
        - Ray Train with data parallelism
        - Automatic checkpointing and progress tracking

    Training Flow:
        1. Load feature engineering output (trades + L2Q)
        2. Calculate mid-price from L2Q (BidPrice1 + AskPrice1) / 2
        3. Extract covariates (volume, spread, imbalances, etc.)
        4. Create time series sequences grouped by ticker
        5. Normalize target and covariates (scale-invariant)
        6. Convert to Chronos2 training format
        7. Train with Ray Train (distributed)
        8. Save final checkpoint to S3

    Covariates:
        Past covariates (known at prediction time):
        - price_close: Last trade price in bar
        - volume: Total trade volume
        - spread_mean: Bid-ask spread
        - volume_imbalance: Buy vs sell volume
        - trade_imbalance: Buy vs sell trade count
        - bid_quantity_mean: Average bid size
        - ask_quantity_mean: Average ask size

    Args:
        model_variant: Chronos2 model to train (default: 'amazon/chronos-2')
        prediction_length: Bars to predict ahead (default: 30, max: 1024)
        context_length: Historical bars for context (default: 512, max: 8192)
        past_covariates: List of covariate column names (default: standard set)
        future_covariates: List of future covariate column names (default: None)
        num_epochs: Training epochs (default: 50, expect days)
        learning_rate: Initial learning rate (default: 1e-4)
        batch_size: Batch size per worker (default: 32)
        num_workers: Number of Ray Train workers (default: 4)
        use_gpu: Enable GPU training (default: True)
        checkpoint_frequency: Save checkpoint every N epochs (default: 5)
        max_retries: Maximum retry attempts (default: 3)
    """

    # Default past covariates from feature engineering
    DEFAULT_PAST_COVARIATES = [
        'price_close',          # Trade price
        'volume',               # Trade volume
        'spread_mean',          # Bid-ask spread
        'volume_imbalance',     # Buy/sell volume imbalance
        'trade_imbalance',      # Buy/sell trade imbalance
        'bid_quantity_mean',    # Average bid size
        'ask_quantity_mean',    # Average ask size
    ]

    def __init__(
        self,
        model_variant: str = 'amazon/chronos-2',
        prediction_length: int = 30,
        context_length: int = 512,
        past_covariates: Optional[List[str]] = None,
        future_covariates: Optional[List[str]] = None,
        num_epochs: int = 50,
        learning_rate: float = 1e-4,
        batch_size: int = 32,
        num_workers: int = 4,
        use_gpu: bool = True,
        checkpoint_frequency: int = 5,
        max_retries: int = 3
    ):
        super().__init__(max_retries)

        # Validate prediction length (Chronos-2 supports up to 1024)
        if not 1 <= prediction_length <= 1024:
            raise ValueError(
                f"prediction_length must be between 1-1024, got {prediction_length}"
            )

        # Validate context length (Chronos-2 supports up to 8192)
        if not 1 <= context_length <= 8192:
            raise ValueError(
                f"context_length must be between 1-8192, got {context_length}"
            )

        # Model configuration
        self.model_variant = model_variant
        self.prediction_length = prediction_length
        self.context_length = context_length

        # Covariate configuration
        self.past_covariates = past_covariates or self.DEFAULT_PAST_COVARIATES
        self.future_covariates = future_covariates or []

        # Training configuration
        self.num_epochs = num_epochs
        self.learning_rate = learning_rate
        self.batch_size = batch_size

        # Ray Train configuration
        self.num_workers = num_workers
        self.use_gpu = use_gpu
        self.checkpoint_frequency = checkpoint_frequency

        # Runtime tracking
        self.training_run_id = datetime.now().strftime("%Y%m%d_%H%M%S")

    def train(self, features: pl.LazyFrame, storage_location: dict) -> Dict[str, Any]:
        """Train global Chronos2 model on all tickers.

        Args:
            features: Feature engineering output (must include L2Q for mid-price)
            storage_location: S3 location for model checkpoint

        Returns:
            Training results with checkpoint path and metrics

        Raises:
            ValueError: If L2Q features are missing or data is insufficient
        """
        print("=" * 80)
        print("CHRONOS2 GLOBAL MODEL TRAINING WITH COVARIATES")
        print("=" * 80)
        print(f"Training run ID: {self.training_run_id}")
        print(f"Model variant: {self.model_variant}")
        print(f"Prediction length: {self.prediction_length} bars")
        print(f"Context length: {self.context_length} bars")
        print(f"Past covariates: {', '.join(self.past_covariates)}")
        if self.future_covariates:
            print(f"Future covariates: {', '.join(self.future_covariates)}")

        # 1. Prepare training data
        print("\n[1/4] Preparing training data...")
        train_dataset, val_dataset, scaler_params = self._prepare_training_data(features)
        print(f"  Covariates per sequence: {len(self.past_covariates)} past, {len(self.future_covariates)} future")

        # 2. Configure Ray Train
        print("\n[2/4] Configuring Ray Train...")
        scaling_config = ScalingConfig(
            num_workers=self.num_workers,
            use_gpu=self.use_gpu,
            resources_per_worker={"GPU": 1} if self.use_gpu else {"CPU": 4}
        )

        checkpoint_config = CheckpointConfig(
            num_to_keep=3,
            checkpoint_frequency=self.checkpoint_frequency,
            checkpoint_score_attribute="val_loss",
            checkpoint_score_order="min"
        )

        run_config = RunConfig(
            name=f"chronos2_training_{self.training_run_id}",
            storage_path=storage_location['path'],
            checkpoint_config=checkpoint_config,
            failure_config=FailureConfig(max_failures=self.max_retries)
        )

        # 3. Create Ray TorchTrainer
        print("\n[3/4] Starting distributed training...")
        print(f"  Workers: {self.num_workers}")
        print(f"  GPUs per worker: {1 if self.use_gpu else 0}")
        print(f"  Estimated training time: {self._estimate_training_time()}")
        print(f"  Checkpoint frequency: every {self.checkpoint_frequency} epochs")

        trainer = TorchTrainer(
            train_loop_per_worker=self._train_loop,
            train_loop_config={
                'model_variant': self.model_variant,
                'prediction_length': self.prediction_length,
                'context_length': self.context_length,
                'past_covariates': self.past_covariates,
                'future_covariates': self.future_covariates,
                'num_epochs': self.num_epochs,
                'learning_rate': self.learning_rate,
                'batch_size': self.batch_size,
                'train_dataset': train_dataset,
                'val_dataset': val_dataset,
                'scaler_params': scaler_params
            },
            scaling_config=scaling_config,
            run_config=run_config
        )

        result = trainer.fit()

        # 4. Finalize and return results
        print("\n[4/4] Training complete!")
        checkpoint_path = result.checkpoint.path
        final_metrics = result.metrics

        print(f"  Final validation loss: {final_metrics['val_loss']:.4f}")
        print(f"  Checkpoint saved: {checkpoint_path}")

        return {
            'status': 'success',
            'training_run_id': self.training_run_id,
            'checkpoint_path': checkpoint_path,
            'val_loss': final_metrics['val_loss'],
            'metrics': final_metrics,
            'model_variant': self.model_variant,
            'prediction_length': self.prediction_length,
            'context_length': self.context_length,
            'past_covariates': self.past_covariates,
            'future_covariates': self.future_covariates,
            'scaler_params': scaler_params
        }

    @staticmethod
    def _train_loop(config: dict):
        """Training loop executed on each Ray Train worker.

        This function runs on each distributed worker with a shard of data.
        Implements full training loop with checkpointing and metrics logging.

        Args:
            config: Training configuration dict with model params and datasets
        """
        import torch
        from torch.nn import MSELoss
        from ray.train import get_context, Checkpoint
        from ray import train
        from chronos import Chronos2Pipeline

        # Get distributed training context
        context = get_context()
        rank = context.get_world_rank()

        if rank == 0:
            print(f"Initializing Chronos-2 model: {config['model_variant']}")

        # Load Chronos-2 model
        device = "cuda" if torch.cuda.is_available() else "cpu"

        pipeline = Chronos2Pipeline.from_pretrained(
            config['model_variant'],
            device_map=device,
            torch_dtype=torch.bfloat16 if device == "cuda" else torch.float32
        )

        # Get underlying model for fine-tuning
        model = pipeline.model
        model.train()

        if rank == 0:
            print(f"Model loaded on device: {device}")
            print(f"Model parameters: {sum(p.numel() for p in model.parameters()):,}")

        # Prepare optimizer
        optimizer = torch.optim.AdamW(
            model.parameters(),
            lr=config['learning_rate']
        )

        # Loss function
        criterion = MSELoss()

        # Get Ray Datasets passed via config
        train_dataset = config['train_dataset']
        val_dataset = config['val_dataset']

        # Resume from checkpoint if available
        checkpoint = train.get_checkpoint()
        start_epoch = 0
        if checkpoint:
            with checkpoint.as_directory() as checkpoint_dir:
                state = torch.load(Path(checkpoint_dir) / "model.pt")
                model.load_state_dict(state['model'])
                optimizer.load_state_dict(state['optimizer'])
                start_epoch = state['epoch'] + 1
                if rank == 0:
                    print(f"Resumed from epoch {start_epoch}")

        # Training loop
        for epoch in range(start_epoch, config['num_epochs']):
            # Train epoch
            model.train()
            train_loss = 0.0
            num_batches = 0

            # Use Ray Data iter_torch_batches for distributed data loading
            for batch in train_dataset.iter_torch_batches(
                batch_size=config['batch_size'],
                dtypes=torch.float16
            ):
                context_tensor = batch['context'].to(device)
                target_tensor = batch['target'].to(device)

                optimizer.zero_grad()

                # Forward pass
                output = model(context_tensor)
                loss = criterion(output, target_tensor)

                # Backward pass
                loss.backward()
                optimizer.step()

                train_loss += loss.item()
                num_batches += 1

            avg_train_loss = train_loss / num_batches if num_batches > 0 else 0.0

            # Validation epoch
            model.eval()
            val_loss = 0.0
            num_val_batches = 0

            with torch.no_grad():
                for batch in val_dataset.iter_torch_batches(
                    batch_size=config['batch_size'],
                    dtypes=torch.float16
                ):
                    context_tensor = batch['context'].to(device)
                    target_tensor = batch['target'].to(device)

                    output = model(context_tensor)
                    loss = criterion(output, target_tensor)

                    val_loss += loss.item()
                    num_val_batches += 1

            avg_val_loss = val_loss / num_val_batches if num_val_batches > 0 else 0.0

            # Log metrics
            metrics = {
                'epoch': epoch,
                'train_loss': avg_train_loss,
                'val_loss': avg_val_loss
            }

            # Save checkpoint
            checkpoint_dir = Path(train.get_context().get_trial_dir()) / "checkpoint"
            checkpoint_dir.mkdir(parents=True, exist_ok=True)

            torch.save(
                {
                    'model': model.state_dict(),
                    'optimizer': optimizer.state_dict(),
                    'epoch': epoch,
                    'scaler_params': config['scaler_params']
                },
                checkpoint_dir / "model.pt"
            )

            # Save metadata
            with open(checkpoint_dir / "metadata.json", 'w') as f:
                json.dump({
                    'model_variant': config['model_variant'],
                    'prediction_length': config['prediction_length'],
                    'context_length': config['context_length'],
                    'past_covariates': config['past_covariates'],
                    'future_covariates': config['future_covariates'],
                    'scaler_params': config['scaler_params']
                }, f, indent=2)

            checkpoint = Checkpoint.from_directory(str(checkpoint_dir))
            train.report(metrics, checkpoint=checkpoint)

            if rank == 0:
                print(f"Epoch {epoch + 1}/{config['num_epochs']}: "
                      f"train_loss={avg_train_loss:.4f}, "
                      f"val_loss={avg_val_loss:.4f}")

    def _prepare_training_data(self, features: pl.LazyFrame):
        """Prepare training data from feature engineering output.

        Steps:
        1. Collect features
        2. Calculate mid-price from L2Q data
        3. Create time series sequences
        4. Normalize features
        5. Split train/val (80/20 temporal)
        6. Convert to Ray Dataset

        Returns:
            Tuple of (train_dataset, val_dataset, scaler_params)
        """
        import ray.data
        from .chronos_training_utils import (
            calculate_mid_price,
            create_time_series_sequences,
            normalize_features,
            temporal_train_val_split
        )

        # Collect features
        print("  Collecting features...")
        features_df = features.collect()
        print(f"  Loaded {len(features_df)} rows")

        # Calculate mid-price (requires L2Q columns)
        print("  Calculating mid-price...")
        features_df = calculate_mid_price(features_df)

        # Create sequences with covariates
        print("  Creating time series sequences...")
        sequences = create_time_series_sequences(
            features_df,
            target_column='mid_price',
            context_length=self.context_length,
            prediction_length=self.prediction_length,
            past_covariates=self.past_covariates,
            future_covariates=self.future_covariates,
            group_by_column='Ticker'
        )

        if not sequences:
            raise ValueError(
                "No sequences created. Check that features have sufficient "
                "data length and required columns."
            )

        # Normalize
        print("  Normalizing features...")
        sequences_norm, scaler_params = normalize_features(sequences)

        # Split
        print("  Splitting train/val...")
        train_seq, val_seq = temporal_train_val_split(
            sequences_norm,
            split_ratio=0.8
        )

        # Convert to Ray Dataset for distributed data loading
        print("  Converting to Ray Dataset...")
        train_dataset = ray.data.from_items(train_seq)
        val_dataset = ray.data.from_items(val_seq)

        print(f"  Train dataset: {train_dataset.count()} sequences")
        print(f"  Val dataset: {val_dataset.count()} sequences")

        return train_dataset, val_dataset, scaler_params

    def _estimate_training_time(self) -> str:
        """Estimate total training time.

        Provides rough estimate based on epochs and workers.
        Actual time depends on hardware, data size, and model complexity.
        """
        # Rough estimate: 1 hour per epoch for medium model on 4 GPUs
        hours = self.num_epochs * 1.0 / self.num_workers
        if hours < 24:
            return f"~{int(hours)} hours"
        else:
            days = hours / 24
            return f"~{days:.1f} days"

    def get_failed_items(self, results: List[Any]) -> List[Any]:
        """Extract failed training runs for retry.

        Since we train a single global model, failure means retrying
        the entire training job, not individual items.

        Args:
            results: Training results

        Returns:
            List of failed items (empty or single item)
        """
        if isinstance(results, dict) and results.get('status') == 'failed':
            return [results]
        return []
