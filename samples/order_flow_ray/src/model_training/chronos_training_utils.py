# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: MIT-0

"""Utility functions for Chronos training data preparation."""
import polars as pl
import numpy as np
from typing import Tuple, List, Dict


def calculate_mid_price(features_df: pl.DataFrame) -> pl.DataFrame:
    """Calculate mid-price from L2Q bid/ask prices.

    Mid-price is the average of the best bid and best ask prices,
    representing the theoretical fair value at each time bar.

    Args:
        features_df: Features with bid_price_mean and ask_price_mean columns

    Returns:
        DataFrame with mid_price column added

    Raises:
        ValueError: If required L2Q columns are missing
    """
    # Check if L2Q columns exist
    required_cols = ['bid_price_mean', 'ask_price_mean']
    missing = [col for col in required_cols if col not in features_df.columns]

    if missing:
        raise ValueError(
            f"Missing L2Q columns for mid-price calculation: {missing}. "
            "Ensure L2Q feature engineering is enabled in pipeline config."
        )

    return features_df.with_columns([
        ((pl.col('bid_price_mean') + pl.col('ask_price_mean')) / 2.0).alias('mid_price')
    ])


def create_time_series_sequences(
    features_df: pl.DataFrame,
    target_column: str,
    context_length: int,
    prediction_length: int,
    past_covariates: List[str] = None,
    future_covariates: List[str] = None,
    group_by_column: str = 'Ticker'
) -> List[Dict]:
    """Create sliding window sequences for time series training with covariates.

    Generates overlapping sequences where each sequence contains:
    - context: Historical target data for model input
    - target: Future target data for model to predict
    - past_covariates: Historical covariate features (known at prediction time)
    - future_covariates: Future covariate features (if available)

    Sequences are created separately for each group (e.g., ticker) to
    prevent mixing data across different time series.

    Args:
        features_df: Bar-level features sorted by time
        target_column: Target variable column name (e.g., 'mid_price')
        context_length: Number of historical bars for context
        prediction_length: Number of future bars to predict
        past_covariates: List of past covariate column names (optional)
        future_covariates: List of future covariate column names (optional)
        group_by_column: Column to group sequences by (default: 'Ticker')

    Returns:
        List of sequence dicts with keys:
        - 'group_id': Group identifier (e.g., ticker symbol)
        - 'context': Historical target values (numpy array)
        - 'target': Future target values (numpy array)
        - 'past_covariates_context': Historical covariates (numpy array, shape: [context_length, n_covariates])
        - 'past_covariates_target': Future covariates for prediction horizon (numpy array)
        - 'future_covariates': Future covariates if provided (numpy array)
        - 'bar_id_start': Starting bar_id for the sequence

    Example:
        For context_length=512, prediction_length=30:
        - Context: bars [0:512] (target + past covariates)
        - Target: bars [512:542] (target + past covariates for horizon)
        - Next sequence starts at bar 1 (sliding window)
    """
    sequences = []
    past_covariates = past_covariates or []
    future_covariates = future_covariates or []

    # Get unique groups
    groups = features_df[group_by_column].unique().to_list()

    print(f"Creating sequences for {len(groups)} groups...")
    if past_covariates:
        print(f"  Past covariates: {', '.join(past_covariates)}")
    if future_covariates:
        print(f"  Future covariates: {', '.join(future_covariates)}")

    for group_id in groups:
        # Filter to single group and sort by time
        group_df = features_df.filter(
            pl.col(group_by_column) == group_id
        ).sort('bar_id')

        # Convert target to numpy for efficient slicing
        target_series = group_df[target_column].to_numpy()

        # Convert covariates to numpy arrays
        past_cov_arrays = {}
        for cov in past_covariates:
            if cov in group_df.columns:
                past_cov_arrays[cov] = group_df[cov].to_numpy()
            else:
                print(f"  Warning: Past covariate '{cov}' not found in data for {group_id}")

        future_cov_arrays = {}
        for cov in future_covariates:
            if cov in group_df.columns:
                future_cov_arrays[cov] = group_df[cov].to_numpy()
            else:
                print(f"  Warning: Future covariate '{cov}' not found in data for {group_id}")

        # Need enough data for at least one sequence
        min_length = context_length + prediction_length
        if len(target_series) < min_length:
            print(f"  Skipping {group_id}: insufficient data "
                  f"({len(target_series)} < {min_length})")
            continue

        # Create sliding windows
        num_sequences = len(target_series) - context_length - prediction_length + 1

        for i in range(num_sequences):
            # Target sequences - use ascontiguousarray for Ray Data compatibility
            context = np.ascontiguousarray(target_series[i:i + context_length])
            target = np.ascontiguousarray(
                target_series[i + context_length:i + context_length + prediction_length]
            )

            seq_dict = {
                'group_id': group_id,
                'context': context,
                'target': target,
                'bar_id_start': int(group_df['bar_id'][i])  # Convert to Python int for serialization
            }

            # Past covariates (context + prediction horizon)
            if past_cov_arrays:
                # Stack covariates into single array [timesteps, n_covariates]
                past_cov_context_list = []
                past_cov_target_list = []

                for cov_name in past_covariates:
                    if cov_name in past_cov_arrays:
                        cov_array = past_cov_arrays[cov_name]
                        past_cov_context_list.append(
                            cov_array[i:i + context_length]
                        )
                        past_cov_target_list.append(
                            cov_array[i + context_length:i + context_length + prediction_length]
                        )

                if past_cov_context_list:
                    seq_dict['past_covariates_context'] = np.ascontiguousarray(
                        np.stack(past_cov_context_list, axis=-1)
                    )
                    seq_dict['past_covariates_target'] = np.ascontiguousarray(
                        np.stack(past_cov_target_list, axis=-1)
                    )

            # Future covariates (if provided)
            if future_cov_arrays:
                future_cov_list = []
                for cov_name in future_covariates:
                    if cov_name in future_cov_arrays:
                        cov_array = future_cov_arrays[cov_name]
                        # Future covariates span context + prediction horizon
                        future_cov_list.append(
                            cov_array[i:i + context_length + prediction_length]
                        )

                if future_cov_list:
                    seq_dict['future_covariates'] = np.ascontiguousarray(
                        np.stack(future_cov_list, axis=-1)
                    )

            sequences.append(seq_dict)

    print(f"Created {len(sequences)} total sequences")
    return sequences


def normalize_features(sequences: List[Dict], eps: float = 1e-8) -> Tuple[List[Dict], Dict]:
    """Normalize sequences to be scale-invariant using z-score normalization.

    Applies global normalization across all sequences:
    - Target: normalized with own mean/std
    - Each covariate: normalized independently with own mean/std

    This ensures the model learns patterns independent of absolute scales,
    which is critical for generalization across different tickers and price ranges.

    Args:
        sequences: List of sequence dicts with 'context', 'target', and optional covariates
        eps: Small epsilon to prevent division by zero (default: 1e-8)

    Returns:
        Tuple of (normalized_sequences, scaler_params)
        - normalized_sequences: List of normalized sequence dicts
        - scaler_params: Dict with 'target' and 'covariates' normalization params

    Note:
        Scaler params should be saved with the model for denormalizing
        predictions during inference.
    """
    # Calculate target statistics
    all_target_values = []
    for seq in sequences:
        all_target_values.extend(seq['context'])
        all_target_values.extend(seq['target'])

    target_mean = np.mean(all_target_values)
    target_std = max(np.std(all_target_values), eps)  # Prevent division by zero

    print(f"Target normalization: mean={target_mean:.4f}, std={target_std:.4f}")

    # Calculate covariate statistics (if present)
    covariate_stats = {}
    has_covariates = 'past_covariates_context' in sequences[0]

    if has_covariates:
        # Get number of covariates
        n_covariates = sequences[0]['past_covariates_context'].shape[-1]

        for cov_idx in range(n_covariates):
            all_cov_values = []
            for seq in sequences:
                # Collect from both context and target horizons
                if 'past_covariates_context' in seq:
                    all_cov_values.extend(seq['past_covariates_context'][:, cov_idx])
                if 'past_covariates_target' in seq:
                    all_cov_values.extend(seq['past_covariates_target'][:, cov_idx])

            cov_mean = np.mean(all_cov_values)
            cov_std = max(np.std(all_cov_values), eps)  # Prevent division by zero
            covariate_stats[f'cov_{cov_idx}'] = {'mean': float(cov_mean), 'std': float(cov_std)}

        print(f"Covariate normalization: {n_covariates} covariates normalized")

    # Normalize all sequences
    normalized_seqs = []
    for seq in sequences:
        norm_seq = {
            'group_id': seq['group_id'],
            'context': (seq['context'] - target_mean) / target_std,
            'target': (seq['target'] - target_mean) / target_std,
            'bar_id_start': seq['bar_id_start']
        }

        # Normalize past covariates
        if 'past_covariates_context' in seq:
            past_cov_context_norm = seq['past_covariates_context'].copy()
            past_cov_target_norm = seq['past_covariates_target'].copy()

            for cov_idx in range(past_cov_context_norm.shape[-1]):
                cov_mean = covariate_stats[f'cov_{cov_idx}']['mean']
                cov_std = covariate_stats[f'cov_{cov_idx}']['std']

                past_cov_context_norm[:, cov_idx] = (
                    past_cov_context_norm[:, cov_idx] - cov_mean
                ) / cov_std
                past_cov_target_norm[:, cov_idx] = (
                    past_cov_target_norm[:, cov_idx] - cov_mean
                ) / cov_std

            norm_seq['past_covariates_context'] = past_cov_context_norm
            norm_seq['past_covariates_target'] = past_cov_target_norm

        # Normalize future covariates
        if 'future_covariates' in seq:
            # Future covariates can use same normalization as past covariates
            # or have their own - for simplicity, use index-based normalization
            future_cov_norm = seq['future_covariates'].copy()
            for cov_idx in range(future_cov_norm.shape[-1]):
                # Use past covariate stats if available, otherwise calculate new
                if f'cov_{cov_idx}' in covariate_stats:
                    cov_mean = covariate_stats[f'cov_{cov_idx}']['mean']
                    cov_std = covariate_stats[f'cov_{cov_idx}']['std']
                else:
                    cov_mean = np.mean(future_cov_norm[:, cov_idx])
                    cov_std = max(np.std(future_cov_norm[:, cov_idx]), eps)  # Prevent division by zero

                future_cov_norm[:, cov_idx] = (
                    future_cov_norm[:, cov_idx] - cov_mean
                ) / cov_std

            norm_seq['future_covariates'] = future_cov_norm

        normalized_seqs.append(norm_seq)

    scaler_params = {
        'target': {'mean': float(target_mean), 'std': float(target_std)},
        'covariates': covariate_stats
    }

    return normalized_seqs, scaler_params


def temporal_train_val_split(
    sequences: List[Dict],
    split_ratio: float = 0.8
) -> Tuple[List[Dict], List[Dict]]:
    """Split sequences temporally without shuffling.

    Maintains temporal order to prevent lookahead bias. Earlier sequences
    go to training, later sequences go to validation.

    This is critical for time series to ensure the model doesn't learn
    from future data during training.

    Args:
        sequences: List of sequence dicts
        split_ratio: Fraction for training (default 0.8 = 80% train, 20% val)

    Returns:
        Tuple of (train_sequences, val_sequences)

    Example:
        If split_ratio=0.8 and 1000 sequences:
        - Train: sequences 0-799 (80%)
        - Val: sequences 800-999 (20%)
    """
    # Sort by bar_id_start to maintain temporal order
    sequences_sorted = sorted(sequences, key=lambda x: x['bar_id_start'])

    split_idx = int(len(sequences_sorted) * split_ratio)
    train_seq = sequences_sorted[:split_idx]
    val_seq = sequences_sorted[split_idx:]

    print(f"Train/val split: {len(train_seq)} train, {len(val_seq)} val")

    return train_seq, val_seq


def denormalize_predictions(
    predictions: np.ndarray,
    scaler_params: Dict
) -> np.ndarray:
    """Denormalize predictions back to original scale.

    Reverses z-score normalization:
    original_value = normalized_value * std + mean

    Args:
        predictions: Normalized predictions
        scaler_params: Dict with 'mean' and 'std' from normalize_features()

    Returns:
        Denormalized predictions in original scale
    """
    mean = scaler_params['mean']
    std = scaler_params['std']

    return predictions * std + mean
