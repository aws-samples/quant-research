# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: MIT-0

import os
import json
import pickle
import torch
import polars as pl
import numpy as np
from typing import List, Dict, Tuple, Any
import logging
from pathlib import Path
from datetime import datetime, timedelta

# Fix import paths to work from any directory
import sys
import os

# Add the current directory to Python path
current_dir = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, current_dir)

# Import from our new modules
from utils import log_memory_usage, force_garbage_collection
from feature_engine import (
    PolarsFeatureEngine,
    clean_features_polars,
    scale_features_polars,
    apply_scaling_polars,
)
from datasets import (
    PolarSequenceDataset,
    LoadedDataset,
    apply_undersampling_to_polar_dataset,
    print_dataset_distribution,
)

# Set up logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger("polars_dataprovider")


class PolarsDataProvider:
    """Memory-efficient data provider using Polars with integrated undersampling."""

    def __init__(self, device, config_dict, region=None):
        """Initialize the data provider with configuration parameters."""
        # Data source configuration
        self.data_path = config_dict["data"].get("data_path", None)
        self.metadata_location = config_dict["data"].get("metadata_location", None)
        self.feature_cols = (
            config_dict["data"]["feature_cols"].split(",")
            if isinstance(config_dict["data"]["feature_cols"], str)
            else config_dict["data"]["feature_cols"]
        )

        # Iceberg configuration - use the region parameter instead of config
        self.region = (
            region if region else config_dict["data"].get("region", "us-east-1")
        )
        self.instrument_filter = config_dict["data"].get("instrument_filter", None)
        if self.instrument_filter:
            if isinstance(self.instrument_filter, str):
                self.instrument_filter = [
                    x.strip() for x in self.instrument_filter.split(",")
                ]

        # Date filtering configuration
        self.start_date = config_dict["data"].get("start_date", None)
        self.end_date = config_dict["data"].get("end_date", None)
        self.date_filter_days = config_dict["data"].get("date_filter_days", None)

        # Model configuration
        self.lookback_period = int(config_dict["data"]["lookback_period"])
        self.lookahead_for_target_calculation = int(
            config_dict["data"]["lookforward_period"]
        )
        self.buy_sell_threshold = int(config_dict["data"]["buy_sell_threshold"])
        self.sequence_length = int(
            config_dict["data"].get("sequence_length", self.lookback_period)
        )

        # Undersampling configuration
        self.apply_undersampling = config_dict["data"].get("apply_undersampling", True)
        self.undersampling_strategy = config_dict["data"].get(
            "undersampling_strategy", "minority_match"
        )  # 'minority_match' or 'balanced'

        # Device
        self.device = device

        # Scaler for feature normalization
        self.scaler = None

        # Runtime environment
        self.local_rank = int(os.environ.get("LOCAL_RANK", "0"))
        self.global_rank = int(os.environ.get("RANK", "0"))
        self.world_size = int(os.environ.get("WORLD_SIZE", "1"))

    def load_and_prepare_data(
        self, save_datasets: bool = True, output_dir: str = "data/prepared_datasets"
    ) -> Dict[str, Any]:
        """Complete data preparation pipeline with undersampling and dataset creation."""
        logger.info("=== Starting Polars-based Data Preparation Pipeline ===")

        # Step 1: Load raw data
        logger.info("Step 1: Loading raw data...")
        raw_df = self._load_data_polars()
        logger.info(f"✓ Loaded {len(raw_df)} rows")

        # Step 2: Add labels
        logger.info("Step 2: Adding target labels...")
        labeled_df = self._add_labels_polars(raw_df)
        logger.info(f"✓ Added labels to {len(labeled_df)} rows")

        # Step 3: Generate features
        logger.info("Step 3: Generating features with Polars...")
        feature_engine = PolarsFeatureEngine()
        feature_df = feature_engine.compute_advanced_features(
            labeled_df, self.lookback_period
        )
        logger.info(f"✓ Generated features: {feature_df.shape}")

        # Step 4: Split data temporally
        logger.info("Step 4: Splitting data temporally...")
        train_df, val_df, test_df = self._temporal_split_polars(feature_df)
        logger.info(
            f"✓ Split - Train: {len(train_df)}, Val: {len(val_df)}, Test: {len(test_df)}"
        )

        # Step 5: Create sequences and apply undersampling
        logger.info("Step 5: Creating sequences and applying undersampling...")
        datasets = self._create_datasets_with_undersampling(train_df, val_df, test_df)

        # Step 6: Save datasets to disk
        if save_datasets:
            logger.info("Step 6: Saving datasets to disk...")
            self._save_datasets(datasets, output_dir)

        logger.info("=== Data Preparation Pipeline Complete ===")
        return datasets

    def _load_data_polars(self) -> pl.DataFrame:
        """Load data using Polars for memory efficiency."""

        # Use metadata_location if available (StaticTable approach)
        if self.metadata_location:
            logger.info(
                f"Loading data from metadata location: {self.metadata_location}"
            )
            return self._load_from_iceberg_static()
        elif self.data_path:
            logger.info(f"Loading data from {self.data_path}")
            if self.data_path.startswith("iceberg://"):
                return self._load_from_iceberg_polars()
            elif self.data_path.startswith("s3://"):
                return self._load_from_s3_polars()
            else:
                return self._load_from_local_polars()
        else:
            raise ValueError(
                "Either metadata_location or data_path must be specified in configuration"
            )

    def _load_from_iceberg_static(self) -> pl.DataFrame:
        """Load from Iceberg using PyIceberg with predicate pushdown, then Arrow->Polars (most memory efficient)."""

        try:
            from pyiceberg.table import StaticTable

            logger.info(
                f"Loading Iceberg table from metadata using PyIceberg with predicate pushdown: {self.metadata_location}"
            )

            # Load table using StaticTable with configured FileIO
            logger.info("Creating StaticTable from metadata...")
            table = StaticTable.from_metadata(self.metadata_location)

            logger.info("✓ Successfully loaded table metadata")
            logger.info(f"Table schema: {table.schema()}")

            # Create scan with PyIceberg (supports true predicate pushdown)
            logger.info("Creating table scan with PyIceberg...")
            scan = table.scan()

            # Apply instrument filters if specified (true predicate pushdown)
            if self.instrument_filter:
                logger.info(
                    f"Applying instrument filter with predicate pushdown: {self.instrument_filter}"
                )
                from pyiceberg.expressions import EqualTo, In

                if len(self.instrument_filter) == 1:
                    scan = scan.filter(EqualTo("instrument", self.instrument_filter[0]))
                else:
                    scan = scan.filter(In("instrument", self.instrument_filter))

            # Apply date filters if specified (true predicate pushdown)
            if self.start_date or self.end_date or self.date_filter_days:
                logger.info("Applying date filters with predicate pushdown...")
                scan = self._apply_iceberg_date_filters(scan)

            logger.info(
                "Converting filtered scan to Arrow (memory efficient with predicate pushdown)..."
            )
            # Convert filtered scan to Arrow (only loads filtered data from storage)
            arrow_table = scan.to_arrow()
            logger.info(
                f"✓ Retrieved {len(arrow_table)} rows from Iceberg using PyIceberg with predicate pushdown"
            )

            if len(arrow_table) == 0:
                logger.warning("⚠️ Iceberg scan returned 0 rows")
                logger.info("This could be due to:")
                logger.info("  1. Applied filters are too restrictive")
                logger.info("  2. No data exists for the specified time range")
                logger.info("  3. Instrument filter doesn't match available data")
                logger.info(
                    f"Current filters: instrument={self.instrument_filter}, dates={self.start_date} to {self.end_date}"
                )

                # Try a scan without filters for debugging
                logger.info("Attempting scan without filters for debugging...")
                try:
                    debug_scan = table.scan().limit(5)
                    debug_arrow = debug_scan.to_arrow()
                    logger.info(f"Sample data without filters: {len(debug_arrow)} rows")
                    if len(debug_arrow) > 0:
                        logger.info(f"Available columns: {debug_arrow.column_names}")
                        if "instrument" in debug_arrow.column_names:
                            unique_instruments = debug_arrow.column(
                                "instrument"
                            ).unique()
                            logger.info(
                                f"Available instruments: {unique_instruments[:10].to_pylist()}"
                            )  # Show first 10
                        if "trade_timestamp" in debug_arrow.column_names:
                            timestamp_col = debug_arrow.column("trade_timestamp")
                            logger.info(
                                f"Date range in data: {timestamp_col.min()} to {timestamp_col.max()}"
                            )
                except Exception as debug_e:
                    logger.warning(f"Debug scan failed: {debug_e}")

            # Convert Arrow to Polars (zero-copy when possible)
            logger.info("Converting Arrow table to Polars DataFrame (zero-copy)...")
            df = pl.from_arrow(arrow_table)

            # Sort by timestamp if available
            if "trade_timestamp" in df.columns:
                df = df.sort("trade_timestamp")
                logger.info("✓ Sorted data by trade_timestamp")

            logger.info(
                f"✓ Successfully loaded {len(df)} rows using PyIceberg → Arrow → Polars pipeline"
            )
            log_memory_usage("after Iceberg data loading")

            return df

        except Exception as e:
            logger.error(
                f"Failed to load from Iceberg using PyIceberg with predicate pushdown: {e}"
            )
            logger.error(f"Error type: {type(e).__name__}")
            logger.error(f"Error details: {str(e)}")

            # Provide troubleshooting information
            logger.error("Troubleshooting tips:")
            logger.error("  1. Verify S3 bucket policy allows cross-account access")
            logger.error("  2. Ensure IAM role has s3:GetObject permissions")
            logger.error("  3. Check if metadata file exists and is accessible")
            logger.error(f"  4. Verify region configuration: {self.region}")
            logger.error(f"  5. Check metadata location: {self.metadata_location}")
            logger.error("  6. Ensure PyIceberg is properly installed")

            raise

    def _load_from_iceberg_polars(self) -> pl.DataFrame:
        """Load from Iceberg using Polars native support (memory efficient)."""
        # Parse the path to get table metadata location
        path = self.data_path.replace("iceberg://", "")

        try:
            logger.info(f"Loading Iceberg table using Polars native support: {path}")

            # For Polars, we need the direct metadata path
            # If path looks like database.table, we need to construct the S3 metadata path
            if "." in path and not path.startswith("s3://"):
                logger.warning(
                    f"Path {path} appears to be a catalog reference, but Polars needs direct metadata path"
                )
                logger.warning(
                    "Please provide metadata_location in config or use direct S3 metadata path"
                )
                raise ValueError(
                    "Polars requires direct S3 metadata path, not catalog reference"
                )

            # Configure storage options for Polars (IAM role handles authentication)
            storage_options = {
                "s3.region": self.region,
            }

            # Assume path is direct S3 metadata path
            metadata_path = path if path.startswith("s3://") else f"s3://{path}"

            logger.info("Creating Polars Iceberg scan (memory efficient)...")

            # Create initial scan using Polars native Iceberg support
            lazy_df = pl.scan_iceberg(metadata_path, storage_options=storage_options)

            # Apply instrument filters if specified
            if self.instrument_filter:
                logger.info(f"Applying instrument filter: {self.instrument_filter}")
                if len(self.instrument_filter) == 1:
                    lazy_df = lazy_df.filter(
                        pl.col("instrument") == self.instrument_filter[0]
                    )
                else:
                    lazy_df = lazy_df.filter(
                        pl.col("instrument").is_in(self.instrument_filter)
                    )

            # Apply date filters if specified
            if self.start_date or self.end_date or self.date_filter_days:
                logger.info("Applying date filters...")
                lazy_df = self._apply_polars_date_filters(lazy_df)

            logger.info("Collecting data using Polars (memory efficient)...")
            # Collect the lazy DataFrame
            df = lazy_df.collect()

            # Sort by timestamp
            if "trade_timestamp" in df.columns:
                df = df.sort("trade_timestamp")

            logger.info(
                f"✓ Loaded {len(df)} rows from Iceberg table using Polars native support"
            )
            return df

        except Exception as e:
            logger.error(
                f"Failed to load from Iceberg using Polars native support: {e}"
            )
            logger.error(
                "Note: Polars requires direct S3 metadata path, not catalog references"
            )
            logger.error(
                "Consider using metadata_location config option with direct S3 path"
            )
            raise

    def _load_from_s3_polars(self) -> pl.DataFrame:
        """Load from S3 using Polars."""
        if self.data_path.endswith(".parquet"):
            df = pl.read_parquet(self.data_path)
        elif self.data_path.endswith(".csv"):
            df = pl.read_csv(self.data_path)
        else:
            raise ValueError(f"Unsupported S3 file format: {self.data_path}")

        return df

    def _load_from_local_polars(self) -> pl.DataFrame:
        """Load from local file using Polars."""
        if self.data_path.endswith(".parquet"):
            df = pl.read_parquet(self.data_path)
        elif self.data_path.endswith(".csv"):
            df = pl.read_csv(self.data_path)
        else:
            raise ValueError(f"Unsupported local file format: {self.data_path}")

        return df

    def _apply_iceberg_date_filters(self, scan):
        """Apply date filtering to Iceberg scan (legacy PyIceberg method)."""
        from pyiceberg.expressions import GreaterThanOrEqual, LessThanOrEqual, And
        from datetime import timezone

        if self.date_filter_days:
            end_timestamp = datetime.now(timezone.utc)
            start_timestamp = end_timestamp - timedelta(days=self.date_filter_days)
            scan = scan.filter(
                And(
                    GreaterThanOrEqual("trade_timestamp", start_timestamp.isoformat()),
                    LessThanOrEqual("trade_timestamp", end_timestamp.isoformat()),
                )
            )
        elif self.start_date and self.end_date:
            start_timestamp = datetime.strptime(self.start_date, "%Y-%m-%d").replace(
                tzinfo=timezone.utc
            )
            end_timestamp = (
                datetime.strptime(self.end_date, "%Y-%m-%d")
                + timedelta(days=1)
                - timedelta(microseconds=1)
            ).replace(tzinfo=timezone.utc)
            scan = scan.filter(
                And(
                    GreaterThanOrEqual("trade_timestamp", start_timestamp.isoformat()),
                    LessThanOrEqual("trade_timestamp", end_timestamp.isoformat()),
                )
            )
        elif self.start_date:
            start_timestamp = datetime.strptime(self.start_date, "%Y-%m-%d").replace(
                tzinfo=timezone.utc
            )
            scan = scan.filter(
                GreaterThanOrEqual("trade_timestamp", start_timestamp.isoformat())
            )
        elif self.end_date:
            end_timestamp = (
                datetime.strptime(self.end_date, "%Y-%m-%d")
                + timedelta(days=1)
                - timedelta(microseconds=1)
            ).replace(tzinfo=timezone.utc)
            scan = scan.filter(
                LessThanOrEqual("trade_timestamp", end_timestamp.isoformat())
            )

        return scan

    def _apply_polars_date_filters(self, lazy_df: pl.LazyFrame) -> pl.LazyFrame:
        """Apply date filtering to Polars LazyFrame (memory efficient)."""
        from datetime import timezone

        if self.date_filter_days:
            end_timestamp = datetime.now(timezone.utc)
            start_timestamp = end_timestamp - timedelta(days=self.date_filter_days)
            lazy_df = lazy_df.filter(
                (pl.col("trade_timestamp") >= start_timestamp)
                & (pl.col("trade_timestamp") <= end_timestamp)
            )
        elif self.start_date and self.end_date:
            start_timestamp = datetime.strptime(self.start_date, "%Y-%m-%d").replace(
                tzinfo=timezone.utc
            )
            end_timestamp = (
                datetime.strptime(self.end_date, "%Y-%m-%d")
                + timedelta(days=1)
                - timedelta(microseconds=1)
            ).replace(tzinfo=timezone.utc)
            lazy_df = lazy_df.filter(
                (pl.col("trade_timestamp") >= start_timestamp)
                & (pl.col("trade_timestamp") <= end_timestamp)
            )
        elif self.start_date:
            start_timestamp = datetime.strptime(self.start_date, "%Y-%m-%d").replace(
                tzinfo=timezone.utc
            )
            lazy_df = lazy_df.filter(pl.col("trade_timestamp") >= start_timestamp)
        elif self.end_date:
            end_timestamp = (
                datetime.strptime(self.end_date, "%Y-%m-%d")
                + timedelta(days=1)
                - timedelta(microseconds=1)
            ).replace(tzinfo=timezone.utc)
            lazy_df = lazy_df.filter(pl.col("trade_timestamp") <= end_timestamp)

        return lazy_df

    def _add_labels_polars(self, df: pl.DataFrame) -> pl.DataFrame:
        """Generate target labels using Polars."""
        logger.info("Adding target labels using Polars...")

        # Calculate bid-ask spread
        df = df.with_columns(
            [(pl.col("ask_price") - pl.col("bid_price")).alias("bid_ask_spread")]
        )

        # Create shifted data for future price movements
        lookahead = self.lookahead_for_target_calculation

        df = df.with_columns(
            [
                # Future prices for target calculation
                pl.col("bid_price")
                .shift(-lookahead)
                .forward_fill()
                .alias("future_bid_price"),
                pl.col("ask_price")
                .shift(-lookahead)
                .forward_fill()
                .alias("future_ask_price"),
            ]
        )

        # Calculate buy and sell targets
        df = df.with_columns(
            [
                (pl.col("future_bid_price") - pl.col("ask_price")).alias("buy_target"),
                (pl.col("bid_price") - pl.col("future_ask_price")).alias("sell_target"),
            ]
        )

        # Create target signal
        trigger_threshold = self.buy_sell_threshold

        df = df.with_columns(
            [
                pl.when(
                    pl.col("buy_target") > trigger_threshold * pl.col("bid_ask_spread")
                )
                .then(1)  # Buy
                .when(
                    pl.col("sell_target") > trigger_threshold * pl.col("bid_ask_spread")
                )
                .then(2)  # Sell
                .otherwise(0)  # Hold
                .alias("target")
            ]
        )

        # Print distribution
        target_counts = df.group_by("target").count().sort("target")
        logger.info("Target label distribution:")
        for row in target_counts.iter_rows():
            target_value, count = row
            label_name = {0: "Hold", 1: "Buy", 2: "Sell"}.get(
                target_value, f"Label_{target_value}"
            )
            percentage = (count / len(df)) * 100
            logger.info(
                f"  {label_name} ({target_value}): {count:,} ({percentage:.2f}%)"
            )

        return df

    def _temporal_split_polars(
        self, df: pl.DataFrame
    ) -> Tuple[pl.DataFrame, pl.DataFrame, pl.DataFrame]:
        """Split data temporally using Polars."""
        # Sort by trade_timestamp
        if "trade_timestamp" in df.columns:
            df = df.sort("trade_timestamp")

        total_rows = len(df)
        train_end = int(0.7 * total_rows)
        val_end = int(0.85 * total_rows)

        train_df = df.slice(0, train_end)
        val_df = df.slice(train_end, val_end - train_end)
        test_df = df.slice(val_end, total_rows - val_end)

        return train_df, val_df, test_df

    def _create_datasets_with_undersampling(
        self, train_df: pl.DataFrame, val_df: pl.DataFrame, test_df: pl.DataFrame
    ) -> Dict[str, Any]:
        """Create PyTorch datasets using memory-efficient Polars processing."""
        logger.info(
            "Creating PyTorch datasets with memory-efficient Polars processing..."
        )
        log_memory_usage("before dataset creation")

        # Identify feature columns (exclude metadata columns)
        exclude_cols = [
            "trade_timestamp",
            "trade_date",
            "date",
            "target",
            "buy_target",
            "sell_target",
            "future_bid_price",
            "future_ask_price",
            "bid_ask_spread",
            "exchange_id",
            "exchange_code",
            "instrument",
            "base_underlying_code",
            "counter_underlying_code",
            "tradeid",
            "exchangetradetimestamp",
            "quote_timestamp",
            "side",
            "bid",
            "ask",
        ]

        feature_cols = [col for col in train_df.columns if col not in exclude_cols]
        logger.info(f"Using {len(feature_cols)} feature columns")

        # ===== MEMORY EFFICIENT APPROACH: Stay in Polars =====

        # Step 1: Clean features in Polars (no pandas conversion!)
        logger.info("Cleaning features using Polars (memory efficient)...")
        train_df = clean_features_polars(train_df, feature_cols)
        log_memory_usage("after cleaning training data")
        force_garbage_collection()

        val_df = clean_features_polars(val_df, feature_cols)
        log_memory_usage("after cleaning validation data")
        force_garbage_collection()

        test_df = clean_features_polars(test_df, feature_cols)
        log_memory_usage("after cleaning test data")
        force_garbage_collection()

        # Step 2: Scale features using Polars (no sklearn!)
        logger.info(
            "Scaling features using Polars MinMax scaling (memory efficient)..."
        )
        train_df, scaling_params = scale_features_polars(train_df, feature_cols)
        self.scaling_params = scaling_params  # Store for reproducibility
        log_memory_usage("after scaling training data")
        force_garbage_collection()

        val_df = apply_scaling_polars(val_df, feature_cols, scaling_params)
        log_memory_usage("after scaling validation data")
        force_garbage_collection()

        test_df = apply_scaling_polars(test_df, feature_cols, scaling_params)
        log_memory_usage("after scaling test data")
        force_garbage_collection()

        # Step 3: Create sequence datasets (minimal pandas conversion)
        logger.info("Creating memory-efficient sequence datasets...")
        datasets = {}

        # Training data with undersampling
        logger.info("Processing training data with sequence dataset...")
        train_dataset = self._create_sequence_dataset(train_df, feature_cols, "target")
        log_memory_usage("after creating training dataset")
        force_garbage_collection()

        if self.apply_undersampling:
            train_dataset = self._apply_undersampling_to_sequence_dataset(train_dataset)
            logger.info(f"✓ Applied undersampling to training data")

        datasets["train"] = train_dataset

        # Validation data
        logger.info("Processing validation data...")
        val_dataset = self._create_sequence_dataset(val_df, feature_cols, "target")
        log_memory_usage("after creating validation dataset")
        force_garbage_collection()

        if self.apply_undersampling:
            val_dataset = self._apply_undersampling_to_sequence_dataset(val_dataset)
            logger.info(
                f"✓ Applied undersampling to validation data for faster training"
            )

        datasets["validation"] = val_dataset

        # Test data
        logger.info("Processing test data...")
        test_dataset = self._create_sequence_dataset(test_df, feature_cols, "target")
        log_memory_usage("after creating test dataset")
        force_garbage_collection()

        if self.apply_undersampling:
            test_dataset = self._apply_undersampling_to_sequence_dataset(test_dataset)
            logger.info(f"✓ Applied undersampling to test data for faster evaluation")

        datasets["test"] = test_dataset

        # Print final distributions
        for split_name, dataset in datasets.items():
            logger.info(f"\n{split_name.capitalize()} dataset:")
            print_dataset_distribution(dataset)

        log_memory_usage("after complete dataset creation")
        return datasets

    def _create_sequences_polars(
        self, df: pl.DataFrame, feature_cols: List[str]
    ) -> Tuple[torch.Tensor, torch.Tensor]:
        """Create sequences from Polars DataFrame."""
        # Convert to numpy for sequence creation
        features = df.select(feature_cols).to_numpy()
        targets = df.select("target").to_numpy().flatten()

        # Handle NaN values
        features = np.nan_to_num(features, nan=0.0, posinf=0.0, neginf=0.0)

        # Normalize features
        feature_medians = np.median(features, axis=0, keepdims=True)
        feature_mads = np.median(
            np.abs(features - feature_medians), axis=0, keepdims=True
        )
        feature_mads = np.where(feature_mads == 0, 1.0, feature_mads)
        features = (features - feature_medians) / (1.4826 * feature_mads)
        features = np.clip(features, -3.0, 3.0)

        # Create sequences
        if len(features) < self.sequence_length:
            # Pad if necessary
            padding_needed = self.sequence_length - len(features)
            padding_features = np.repeat(features[0:1], padding_needed, axis=0)
            features = np.vstack([padding_features, features])

            padding_targets = np.repeat(targets[0:1], padding_needed, axis=0)
            targets = np.hstack([padding_targets, targets])

        # Create sliding windows
        num_sequences = len(features) - self.sequence_length
        sequences = np.zeros((num_sequences, self.sequence_length, features.shape[1]))

        for i in range(num_sequences):
            sequences[i] = features[i : i + self.sequence_length]

        # Align targets
        aligned_targets = targets[self.sequence_length :]

        # Convert to tensors
        sequences_tensor = torch.tensor(
            sequences, dtype=torch.float32, device=self.device
        )
        targets_tensor = torch.tensor(
            aligned_targets, dtype=torch.long, device=self.device
        )

        return sequences_tensor, targets_tensor

    def _apply_undersampling(
        self, sequences: torch.Tensor, targets: torch.Tensor
    ) -> Tuple[torch.Tensor, torch.Tensor]:
        """Apply undersampling to balance class distribution while preserving temporal relations."""
        logger.info("Applying undersampling to training sequences...")

        # Get class distribution
        unique_targets, target_counts = torch.unique(targets, return_counts=True)
        logger.info(
            f"Original distribution: {dict(zip(unique_targets.tolist(), target_counts.tolist()))}"
        )

        if self.undersampling_strategy == "minority_match":
            # Match minority class count
            minority_count = torch.min(target_counts)
            logger.info(f"Undersampling to minority class count: {minority_count}")

            # Collect indices for each class
            sampled_indices = []
            for target_value in unique_targets:
                class_mask = targets == target_value
                class_indices = torch.nonzero(class_mask, as_tuple=True)[0]

                if len(class_indices) > minority_count:
                    # Randomly sample from this class
                    perm = torch.randperm(len(class_indices))
                    selected = class_indices[perm[:minority_count]]
                else:
                    # Use all samples if less than minority count
                    selected = class_indices

                sampled_indices.append(selected)

            # Combine all sampled indices
            all_indices = torch.cat(sampled_indices)

        elif self.undersampling_strategy == "balanced":
            # Create perfectly balanced dataset
            min_count = torch.min(target_counts)
            sampled_indices = []

            for target_value in unique_targets:
                class_mask = targets == target_value
                class_indices = torch.nonzero(class_mask, as_tuple=True)[0]

                # Sample exactly min_count from each class
                if len(class_indices) >= min_count:
                    perm = torch.randperm(len(class_indices))
                    selected = class_indices[perm[:min_count]]
                    sampled_indices.append(selected)

            all_indices = torch.cat(sampled_indices)

        else:
            raise ValueError(
                f"Unknown undersampling strategy: {self.undersampling_strategy}"
            )

        # Sort indices to maintain some temporal order
        all_indices = torch.sort(all_indices)[0]

        # Apply undersampling
        undersampled_sequences = sequences[all_indices]
        undersampled_targets = targets[all_indices]

        # Log final distribution
        final_unique, final_counts = torch.unique(
            undersampled_targets, return_counts=True
        )
        logger.info(
            f"Final distribution after undersampling: {dict(zip(final_unique.tolist(), final_counts.tolist()))}"
        )
        logger.info(
            f"Reduced from {len(sequences)} to {len(undersampled_sequences)} sequences"
        )

        return undersampled_sequences, undersampled_targets

    def _create_sequence_dataset(
        self, df: pl.DataFrame, features: List[str], target: str
    ):
        """Create a truly memory-efficient sequence dataset from Polars DataFrame (NO pandas conversion)."""
        logger.info(
            "Creating sequence dataset (memory efficient - no pandas conversion)..."
        )

        # Use the new PolarSequenceDataset that works directly with Polars DataFrames
        dataset = PolarSequenceDataset(
            df=df,
            features=features,
            target=target,
            sequence_length=self.sequence_length,
            device=self.device,
        )

        # Add compatibility attributes for undersampling methods
        dataset.X = None  # Will be computed on-demand
        dataset.y = None  # Will be computed on-demand

        return dataset

    def _apply_undersampling_to_sequence_dataset(self, dataset):
        """Apply undersampling to a sequence dataset."""
        logger.info("Applying undersampling to sequence dataset...")

        # Check if this is a PolarSequenceDataset (sequence)
        if isinstance(dataset, PolarSequenceDataset):
            return apply_undersampling_to_polar_dataset(
                dataset, self.undersampling_strategy
            )

        # For pre-computed datasets, use the legacy approach
        from datasets import apply_undersampling, UnderSampledDataset

        sequences = dataset.X
        targets = dataset.y

        # Apply undersampling logic
        undersampled_sequences, undersampled_targets = apply_undersampling(
            sequences, targets, self.undersampling_strategy
        )

        return UnderSampledDataset(undersampled_sequences, undersampled_targets)

    def _print_dataset_distribution(self, dataset):
        """Print class distribution for a dataset."""
        targets = None

        # Handle different dataset types
        if hasattr(dataset, "y") and dataset.y is not None:
            targets = dataset.y.cpu()
        elif hasattr(dataset, "targets") and dataset.targets is not None:
            targets = dataset.targets.cpu()
        elif isinstance(dataset, PolarSequenceDataset):
            # For streaming datasets, extract targets from the Polars DataFrame
            logger.info(
                "  Extracting targets from streaming dataset for distribution analysis..."
            )
            targets_array = dataset.df[dataset.sequence_length :][
                dataset.target
            ].to_numpy()
            targets = torch.tensor(targets_array, dtype=torch.long, device="cpu")
        elif hasattr(dataset, "original_dataset") and isinstance(
            dataset.original_dataset, PolarSequenceDataset
        ):
            # For undersampled streaming datasets, use the selected indices
            logger.info("  Extracting targets from undersampled streaming dataset...")
            original_dataset = dataset.original_dataset
            targets_array = original_dataset.df[original_dataset.sequence_length :][
                original_dataset.target
            ].to_numpy()
            all_targets = torch.tensor(targets_array, dtype=torch.long, device="cpu")
            # Get only the undersampled targets
            targets = all_targets[dataset.selected_indices]
        else:
            logger.warning(
                "Could not find targets in dataset for distribution analysis"
            )
            logger.info(f"  Total samples: {len(dataset):,}")
            return

        if targets is None:
            logger.warning("Could not extract targets for distribution analysis")
            logger.info(f"  Total samples: {len(dataset):,}")
            return

        unique_targets, target_counts = torch.unique(targets, return_counts=True)

        total_samples = len(targets)
        class_names = ["Hold", "Buy", "Sell"]

        logger.info(f"  Total samples: {total_samples:,}")
        for target, count in zip(unique_targets, target_counts):
            class_name = (
                class_names[target] if target < len(class_names) else f"Class_{target}"
            )
            percentage = (count.item() / total_samples) * 100
            logger.info(f"    {class_name}: {count.item():,} ({percentage:.1f}%)")

    def _save_datasets(self, datasets: Dict[str, Any], output_dir: str):
        """Save prepared datasets to disk for fast loading during training."""
        logger.info(f"Saving datasets to {output_dir}...")

        # Create output directory
        Path(output_dir).mkdir(parents=True, exist_ok=True)

        # Save each dataset
        for split_name, dataset in datasets.items():
            dataset_path = Path(output_dir) / f"{split_name}_dataset.pt"

            # Get sequences and targets using the correct attribute names
            if hasattr(dataset, "X") and hasattr(dataset, "y"):
                sequences = dataset.X
                targets = dataset.y
            elif hasattr(dataset, "sequences") and hasattr(dataset, "targets"):
                sequences = dataset.sequences
                targets = dataset.targets
            else:
                logger.error(f"Dataset {split_name} has unknown attribute structure")
                continue

            # Save as PyTorch tensors
            torch.save(
                {
                    "sequences": sequences,
                    "targets": targets,
                    "metadata": {
                        "split": split_name,
                        "num_samples": len(dataset),
                        "sequence_length": self.sequence_length,
                        "num_features": sequences.shape[2],
                        "created_at": datetime.now().isoformat(),
                        "undersampling_applied": self.apply_undersampling
                        and split_name == "train",
                        "undersampling_strategy": (
                            self.undersampling_strategy
                            if self.apply_undersampling
                            else None
                        ),
                    },
                },
                dataset_path,
            )

            logger.info(
                f"✓ Saved {split_name} dataset: {dataset_path} ({len(dataset):,} samples)"
            )

        # Save configuration for reproducibility
        config_path = Path(output_dir) / "preparation_config.json"
        config_data = {
            "data_path": self.data_path,
            "sequence_length": self.sequence_length,
            "lookback_period": self.lookback_period,
            "lookahead_for_target_calculation": self.lookahead_for_target_calculation,
            "buy_sell_threshold": self.buy_sell_threshold,
            "apply_undersampling": self.apply_undersampling,
            "undersampling_strategy": self.undersampling_strategy,
            "device": str(self.device),
            "created_at": datetime.now().isoformat(),
        }

        with open(config_path, "w") as f:
            json.dump(config_data, f, indent=2)

        logger.info(f"✓ Saved preparation config: {config_path}")

        # Save the scaler that was used for feature scaling
        if self.scaler is not None:
            scaler_path = Path(output_dir) / "scaler.pkl"
            with open(scaler_path, "wb") as f:
                pickle.dump(self.scaler, f)
            logger.info(f"✓ Saved MinMaxScaler to: {scaler_path}")
        else:
            logger.warning("⚠️ No scaler was created during data preparation")

        logger.info(f"✓ All datasets saved to: {output_dir}")

    @staticmethod
    def load_prepared_datasets(datasets_dir: str, device: str = None) -> Dict[str, Any]:
        """Load previously prepared datasets from disk."""
        logger.info(f"Loading prepared datasets from {datasets_dir}...")

        if device is None:
            device = torch.device("cuda" if torch.cuda.is_available() else "cpu")

        datasets = {}
        datasets_path = Path(datasets_dir)

        # Load each dataset file
        for split_name in ["train", "validation", "test"]:
            dataset_file = datasets_path / f"{split_name}_dataset.pt"

            if dataset_file.exists():
                logger.info(f"Loading {split_name} dataset...")
                data = torch.load(dataset_file, map_location=device)

                # Create dataset with loaded tensors
                dataset = LoadedDataset(
                    sequences=data["sequences"].to(device),
                    targets=data["targets"].to(device),
                )

                datasets[split_name] = dataset

                # Log metadata
                metadata = data.get("metadata", {})
                logger.info(
                    f"  ✓ {split_name}: {len(dataset):,} samples, "
                    f"seq_len={metadata.get('sequence_length', 'unknown')}, "
                    f"features={metadata.get('num_features', 'unknown')}"
                )

                if metadata.get("undersampling_applied"):
                    logger.info(
                        f"    Undersampling: {metadata.get('undersampling_strategy', 'unknown')}"
                    )
            else:
                logger.warning(f"Dataset file not found: {dataset_file}")

        if not datasets:
            raise FileNotFoundError(f"No dataset files found in {datasets_dir}")

        logger.info(f"✓ Loaded {len(datasets)} datasets successfully")
        return datasets


def prepare_data_pipeline(
    config_dict: Dict[str, Any],
    save_datasets: bool = True,
    output_dir: str = "data/prepared_datasets",
) -> Dict[str, Any]:
    """Standalone function to run the complete data preparation pipeline."""
    logger.info("=== Starting Standalone Data Preparation Pipeline ===")

    # Set device
    device = torch.device("cuda" if torch.cuda.is_available() else "cpu")
    logger.info(f"Using device: {device}")

    # Initialize data provider
    data_provider = PolarsDataProvider(device, config_dict)

    # Run pipeline
    datasets = data_provider.load_and_prepare_data(
        save_datasets=save_datasets, output_dir=output_dir
    )

    logger.info("=== Data Preparation Pipeline Complete ===")

    # Return summary information
    train_dataset = datasets["train"]

    # Get sequences using the correct attribute name
    if hasattr(train_dataset, "X"):
        sequences = train_dataset.X
    elif hasattr(train_dataset, "sequences"):
        sequences = train_dataset.sequences
    else:
        raise AttributeError("Could not find sequences in train dataset")

    summary = {
        "datasets": datasets,
        "output_dir": output_dir if save_datasets else None,
        "device": str(device),
        "num_features": sequences.shape[2],
        "sequence_length": sequences.shape[1],
        "splits": {
            split_name: len(dataset) for split_name, dataset in datasets.items()
        },
    }

    return summary


def main():
    """Main function to test the Polars data preparation pipeline."""
    import argparse

    parser = argparse.ArgumentParser(description="Polars Data Preparation Pipeline")
    parser.add_argument(
        "--config",
        type=str,
        default="configs/date_filtering_config.json",
        help="Configuration file",
    )
    parser.add_argument(
        "--output-dir",
        type=str,
        default="data/prepared_datasets",
        help="Output directory for datasets",
    )
    parser.add_argument(
        "--no-save", action="store_true", help="Do not save datasets to disk"
    )

    args = parser.parse_args()

    try:
        # Load configuration
        logger.info(f"Loading configuration from {args.config}...")

        if not os.path.exists(args.config):
            logger.warning(
                f"Config file {args.config} not found, using default configuration"
            )
            config_dict = {
                "data": {
                    "data_path": "iceberg://crypto.sample",
                    "feature_cols": "trade_price,trade_quantity,bid_price,ask_price",
                    "region": "us-east-1",
                    "lookback_period": 128,
                    "lookforward_period": 10,
                    "buy_sell_threshold": 2,
                    "sequence_length": 128,
                    "date_filter_days": 7,
                    "instrument_filter": "72089",
                    "apply_undersampling": True,
                    "undersampling_strategy": "minority_match",
                }
            }
        else:
            with open(args.config, "r") as f:
                config_dict = json.load(f)
            logger.info(f"✓ Loaded configuration from {args.config}")

        # Run pipeline
        summary = prepare_data_pipeline(
            config_dict=config_dict,
            save_datasets=not args.no_save,
            output_dir=args.output_dir,
        )

        # Print summary
        logger.info("\n=== PIPELINE SUMMARY ===")
        logger.info(f"Device: {summary['device']}")
        logger.info(f"Sequence Length: {summary['sequence_length']}")
        logger.info(f"Number of Features: {summary['num_features']}")
        logger.info(f"Dataset Splits:")
        for split_name, count in summary["splits"].items():
            logger.info(f"  {split_name.capitalize()}: {count:,} samples")

        if summary["output_dir"]:
            logger.info(f"Datasets saved to: {summary['output_dir']}")

        logger.info("\n✓ Data preparation pipeline completed successfully!")

    except Exception as e:
        logger.error(f"Pipeline failed: {e}")
        import traceback

        traceback.print_exc()
        return 1

    return 0


if __name__ == "__main__":
    exit(main())
