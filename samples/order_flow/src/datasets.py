# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: MIT-0

"""
Dataset classes for memory-efficient sequence data handling.
"""

import time
import torch
import polars as pl
from torch.utils.data import Dataset
from typing import List
import logging

# Fix import paths to work from any directory
import sys
import os

# Add the current directory to Python path
current_dir = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, current_dir)

from utils import log_memory_usage, force_garbage_collection

logger = logging.getLogger(__name__)


class PolarSequenceDataset(Dataset):
    """Memory-efficient sequence dataset that works directly with Polars DataFrames."""

    def __init__(
        self,
        df: pl.DataFrame,
        features: List[str],
        target: str,
        sequence_length: int = 128,
        device: str = "cpu",
    ):
        self.df = df
        self.features = features
        self.target = target
        self.sequence_length = sequence_length
        self.device = device

        # Calculate valid sequence indices
        self.length = max(0, len(df) - sequence_length)

        logger.info(
            f"✓ Created Polars sequence dataset with {self.length:,} sequences (memory efficient)"
        )
        log_memory_usage("after Polars sequence dataset creation")

    def __len__(self):
        return self.length

    def __getitem__(self, idx):
        """Extract sequence on-demand (memory efficient)."""
        if idx >= self.length:
            raise IndexError(
                f"Index {idx} out of range for dataset of length {self.length}"
            )

        # Extract sequence data directly from Polars (no intermediate pandas!)
        sequence_slice = self.df[idx : idx + self.sequence_length]

        # Convert only the small sequence to numpy/tensor
        sequence_data = sequence_slice.select(self.features).to_numpy()
        target_data = self.df[idx + self.sequence_length][self.target]

        # Convert to tensors
        sequence_tensor = torch.tensor(
            sequence_data, dtype=torch.float32, device=self.device
        )
        target_tensor = torch.tensor(target_data, dtype=torch.long, device=self.device)

        return sequence_tensor, target_tensor


class OptimizedSequenceDataset(Dataset):
    """Ultra-fast sequence dataset using pre-computed sequences with PyTorch optimizations."""

    def __init__(
        self, device, dataframe, target, features, sequence_length=128, method="strided"
    ):
        self.features = features
        self.target = target
        self.sequence_length = sequence_length
        self.device = device
        self.method = method

        logger.info(f"Creating optimized sequence dataset with method: {method}")
        log_memory_usage("before sequence creation")
        start_time = time.time()

        # Convert to tensors on CPU (avoid CUDA multiprocessing issues)
        feature_data = torch.tensor(
            dataframe[features].values, dtype=torch.float32, device="cpu"
        )
        target_data = torch.tensor(
            dataframe[target].values, dtype=torch.long, device="cpu"
        )

        # Clear source data to free memory
        del dataframe
        force_garbage_collection()

        # Pre-compute all sequences using strided method (most efficient)
        self.X, self.y = self._create_sequences_strided(feature_data, target_data)

        # Clear temporary tensors
        del feature_data, target_data
        force_garbage_collection()

        creation_time = time.time() - start_time
        logger.info(f"✓ Created {len(self)} sequences in {creation_time:.3f} seconds")
        logger.info(f"✓ Sequence shape: {self.X.shape}, Target shape: {self.y.shape}")
        logger.info(
            f"✓ Memory usage: {self.X.numel() * 4 / 1024**3:.2f} GB (sequences) + {self.y.numel() * 8 / 1024**3:.3f} GB (targets)"
        )
        log_memory_usage("after sequence creation")

    def _create_sequences_strided(self, features, targets):
        """Create sequences using torch.as_strided() - most memory efficient."""
        logger.info("Using torch.as_strided() method for sequence creation...")

        if len(features) < self.sequence_length:
            # Pad if necessary
            padding_needed = self.sequence_length - len(features)
            padding = features[0:1].repeat(padding_needed, 1)
            features = torch.cat([padding, features], dim=0)

            padding_targets = targets[0:1].repeat(padding_needed)
            targets = torch.cat([padding_targets, targets], dim=0)

        # Calculate strides for creating sliding windows
        num_sequences = len(features) - self.sequence_length
        num_features = features.shape[1]

        if num_sequences <= 0:
            # If not enough data, create a single padded sequence
            sequences = torch.zeros(
                (1, self.sequence_length, num_features),
                dtype=torch.float32,
                device="cpu",
            )
            aligned_targets = targets[:1]
        else:
            # Create strided tensor for sequences
            # This creates a view of the data without copying
            sequences = torch.as_strided(
                features,
                size=(num_sequences, self.sequence_length, num_features),
                stride=(features.stride(0), features.stride(0), features.stride(1)),
            )

            # Align targets (to match original implementation)
            aligned_targets = targets[self.sequence_length :]

        return sequences, aligned_targets

    def __len__(self):
        return len(self.y)

    def __getitem__(self, idx):
        # No computation needed - sequences are pre-computed!
        return self.X[idx], self.y[idx]


class UnderSampledDataset(Dataset):
    """Dataset wrapper for undersampled data with compatibility for different underlying datasets."""

    def __init__(self, sequences, targets):
        self.X = sequences
        self.y = targets
        self.sequences = sequences  # For compatibility
        self.targets = targets  # For compatibility

    def __len__(self):
        return len(self.y)

    def __getitem__(self, idx):
        return self.X[idx], self.y[idx]


class UnderSampledPolarDataset(Dataset):
    """Undersampled sequence dataset that maps indices to original dataset."""

    def __init__(self, original_dataset, selected_indices):
        self.original_dataset = original_dataset
        self.selected_indices = selected_indices.tolist()
        self.length = len(self.selected_indices)

        # Compatibility attributes (computed on-demand)
        self.X = None
        self.y = None

    def __len__(self):
        return self.length

    def __getitem__(self, idx):
        # Map to original dataset index
        original_idx = self.selected_indices[idx]
        return self.original_dataset[original_idx]


def apply_undersampling(
    sequences: torch.Tensor, targets: torch.Tensor, strategy: str = "minority_match"
) -> tuple[torch.Tensor, torch.Tensor]:
    """Apply undersampling to balance class distribution while preserving temporal relations."""
    logger.info("Applying undersampling to training sequences...")

    # Get class distribution
    unique_targets, target_counts = torch.unique(targets, return_counts=True)
    logger.info(
        f"Original distribution: {dict(zip(unique_targets.tolist(), target_counts.tolist()))}"
    )

    if strategy == "minority_match":
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

    elif strategy == "balanced":
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
        raise ValueError(f"Unknown undersampling strategy: {strategy}")

    # Sort indices to maintain some temporal order
    all_indices = torch.sort(all_indices)[0]

    # Apply undersampling
    undersampled_sequences = sequences[all_indices]
    undersampled_targets = targets[all_indices]

    # Log final distribution
    final_unique, final_counts = torch.unique(undersampled_targets, return_counts=True)
    logger.info(
        f"Final distribution after undersampling: {dict(zip(final_unique.tolist(), final_counts.tolist()))}"
    )
    logger.info(
        f"Reduced from {len(sequences)} to {len(undersampled_sequences)} sequences"
    )

    return undersampled_sequences, undersampled_targets


def apply_undersampling_to_polar_dataset(
    dataset: PolarSequenceDataset, strategy: str = "minority_match"
) -> UnderSampledPolarDataset:
    """Apply undersampling to a PolarSequenceDataset (sequence dataset)."""
    logger.info("Applying undersampling to PolarSequenceDataset (memory efficient)...")

    # Extract targets from the Polars DataFrame for undersampling analysis
    logger.info("Extracting targets for undersampling analysis...")
    targets_array = dataset.df[dataset.sequence_length :][dataset.target].to_numpy()
    targets_tensor = torch.tensor(targets_array, dtype=torch.long, device="cpu")

    # Get class distribution
    unique_targets, target_counts = torch.unique(targets_tensor, return_counts=True)
    logger.info(
        f"Original distribution: {dict(zip(unique_targets.tolist(), target_counts.tolist()))}"
    )

    # Calculate undersampling indices
    if strategy == "minority_match":
        # Match minority class count
        minority_count = torch.min(target_counts)
        logger.info(f"Undersampling to minority class count: {minority_count}")

        # Collect indices for each class
        sampled_indices = []
        for target_value in unique_targets:
            class_mask = targets_tensor == target_value
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

    elif strategy == "balanced":
        # Create perfectly balanced dataset
        min_count = torch.min(target_counts)
        sampled_indices = []

        for target_value in unique_targets:
            class_mask = targets_tensor == target_value
            class_indices = torch.nonzero(class_mask, as_tuple=True)[0]

            if len(class_indices) >= min_count:
                perm = torch.randperm(len(class_indices))
                selected = class_indices[perm[:min_count]]
                sampled_indices.append(selected)

        all_indices = torch.cat(sampled_indices)

    else:
        raise ValueError(f"Unknown undersampling strategy: {strategy}")

    # Sort indices to maintain some temporal order
    all_indices = torch.sort(all_indices)[0]

    # Log final distribution
    final_targets = targets_tensor[all_indices]
    final_unique, final_counts = torch.unique(final_targets, return_counts=True)
    logger.info(
        f"Final distribution after undersampling: {dict(zip(final_unique.tolist(), final_counts.tolist()))}"
    )
    logger.info(f"Reduced from {len(targets_tensor)} to {len(all_indices)} sequences")

    return UnderSampledPolarDataset(dataset, all_indices)


class LoadedDataset(Dataset):
    """Simple dataset class for loading prepared datasets from disk."""

    def __init__(self, sequences, targets):
        self.X = sequences
        self.y = targets
        self.sequences = sequences  # For compatibility
        self.targets = targets  # For compatibility

    def __len__(self):
        return len(self.y)

    def __getitem__(self, idx):
        return self.X[idx], self.y[idx]


def print_dataset_distribution(dataset):
    """Print class distribution for a dataset."""
    targets = None

    # Handle different dataset types
    if hasattr(dataset, "y") and dataset.y is not None:
        targets = dataset.y.cpu()
    elif hasattr(dataset, "targets") and dataset.targets is not None:
        targets = dataset.targets.cpu()
    elif isinstance(dataset, PolarSequenceDataset):
        # For sequence datasets, extract targets from the Polars DataFrame
        logger.info(
            "  Extracting targets from sequence dataset for distribution analysis..."
        )
        targets_array = dataset.df[dataset.sequence_length :][dataset.target].to_numpy()
        targets = torch.tensor(targets_array, dtype=torch.long, device="cpu")
    elif hasattr(dataset, "original_dataset") and isinstance(
        dataset.original_dataset, PolarSequenceDataset
    ):
        # For undersampled sequence datasets, use the selected indices
        logger.info("  Extracting targets from undersampled sequence dataset...")
        original_dataset = dataset.original_dataset
        targets_array = original_dataset.df[original_dataset.sequence_length :][
            original_dataset.target
        ].to_numpy()
        all_targets = torch.tensor(targets_array, dtype=torch.long, device="cpu")
        # Get only the undersampled targets
        targets = all_targets[dataset.selected_indices]
    else:
        logger.warning("Could not find targets in dataset for distribution analysis")
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
