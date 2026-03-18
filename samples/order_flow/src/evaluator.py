# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: MIT-0

"""
Model evaluation and metrics calculation module.
"""

import time
import numpy as np
import torch
import torch.nn as nn
from torch.utils.data import DataLoader
from sklearn.metrics import (
    accuracy_score,
    precision_score,
    recall_score,
    f1_score,
    classification_report,
    confusion_matrix,
    roc_auc_score,
)
from typing import Dict, Any
import logging

# Fix import paths to work from any directory
import sys
import os

# Add the current directory to Python path
current_dir = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, current_dir)

from model_factory import get_model_forward_pass

logger = logging.getLogger(__name__)


class ModelEvaluator:
    """Handles model evaluation on different dataset splits."""

    def __init__(self, device: str = "cpu"):
        self.device = torch.device(device)
        self.class_names = ["Hold", "Buy", "Sell"]

    def evaluate_model(
        self, model: nn.Module, dataset, split: str = "test", batch_size: int = 512
    ) -> Dict[str, Any]:
        """Evaluate model on specified dataset split."""
        logger.info(f"Evaluating model on {split} set...")

        # Create dataloader with optimized settings for evaluation
        dataloader = DataLoader(
            dataset,
            batch_size=batch_size,
            shuffle=False,
            pin_memory=False,  # Disable to avoid CUDA tensor pinning errors
            num_workers=0,  # Disable to avoid CUDA multiprocessing errors
        )

        logger.info(
            f"Using evaluation batch size: {batch_size} for faster {split} evaluation"
        )

        model.eval()

        all_predictions = []
        all_targets = []
        all_probabilities = []
        inference_times = []

        with torch.no_grad():
            for inputs, targets in dataloader:
                inputs = inputs.to(self.device)
                targets = targets.to(self.device)

                # Measure inference time
                start_time = time.time()

                # Use model factory forward pass handler
                outputs = get_model_forward_pass(model, inputs)

                inference_time = time.time() - start_time
                inference_times.append(inference_time)

                # Get predictions and probabilities
                probabilities = torch.softmax(outputs, dim=1)
                _, predictions = torch.max(outputs, dim=1)

                all_predictions.extend(predictions.cpu().numpy())
                all_targets.extend(targets.cpu().numpy())
                all_probabilities.extend(probabilities.cpu().numpy())

        # Convert to numpy arrays
        all_predictions = np.array(all_predictions)
        all_targets = np.array(all_targets)
        all_probabilities = np.array(all_probabilities)

        # Calculate comprehensive metrics
        metrics = self._calculate_metrics(
            all_targets, all_predictions, all_probabilities, inference_times
        )

        # Log summary
        logger.info(f"✓ {split.capitalize()} evaluation complete:")
        logger.info(f"  Accuracy: {metrics['accuracy']:.4f}")
        logger.info(f"  F1 Score: {metrics['f1_macro']:.4f}")
        logger.info(
            f"  Inference: {metrics['avg_inference_time']*1000:.2f}ms avg, {metrics['inference_samples_per_second']:.0f} samples/sec"
        )

        return metrics

    def _calculate_metrics(
        self, targets, predictions, probabilities, inference_times
    ) -> Dict[str, Any]:
        """Calculate comprehensive evaluation metrics."""
        metrics = {
            # Core metrics
            "accuracy": accuracy_score(targets, predictions),
            "precision_macro": precision_score(
                targets, predictions, average="macro", zero_division=0
            ),
            "recall_macro": recall_score(
                targets, predictions, average="macro", zero_division=0
            ),
            "f1_macro": f1_score(
                targets, predictions, average="macro", zero_division=0
            ),
            # Performance metrics
            "avg_inference_time": np.mean(inference_times),
            "total_inference_time": np.sum(inference_times),
            "inference_samples_per_second": (
                len(predictions) / np.sum(inference_times)
                if np.sum(inference_times) > 0
                else 0
            ),
        }

        # Per-class metrics
        for class_idx, class_name in enumerate(self.class_names):
            class_mask = targets == class_idx
            if np.sum(class_mask) > 0:
                class_precision = precision_score(
                    targets,
                    predictions,
                    labels=[class_idx],
                    average=None,
                    zero_division=0,
                )
                class_recall = recall_score(
                    targets,
                    predictions,
                    labels=[class_idx],
                    average=None,
                    zero_division=0,
                )
                class_f1 = f1_score(
                    targets,
                    predictions,
                    labels=[class_idx],
                    average=None,
                    zero_division=0,
                )

                metrics[f"precision_{class_name}"] = (
                    class_precision[0] if len(class_precision) > 0 else 0.0
                )
                metrics[f"recall_{class_name}"] = (
                    class_recall[0] if len(class_recall) > 0 else 0.0
                )
                metrics[f"f1_{class_name}"] = class_f1[0] if len(class_f1) > 0 else 0.0

        # Multi-class AUC
        try:
            metrics["auc_macro"] = roc_auc_score(
                targets, probabilities, multi_class="ovr", average="macro"
            )
        except Exception as e:
            logger.warning(f"Could not calculate AUC: {e}")
            metrics["auc_macro"] = 0.0

        # Confusion matrix
        cm = confusion_matrix(targets, predictions)
        metrics["confusion_matrix"] = cm.tolist()

        return metrics

    def show_class_distributions(self, datasets: Dict[str, Any]):
        """Show class distribution statistics for all dataset splits."""
        logger.info("\n" + "=" * 60)
        logger.info("CLASS DISTRIBUTION ANALYSIS")
        logger.info("=" * 60)

        class_names = ["Hold (0)", "Buy (1)", "Sell (2)"]

        for split_name, dataset in datasets.items():
            logger.info(f"\n{split_name.upper()} SET:")

            # Get targets using correct attribute name
            if hasattr(dataset, "y"):
                targets = dataset.y.cpu().numpy()
            elif hasattr(dataset, "targets"):
                targets = dataset.targets.cpu().numpy()
            else:
                logger.warning(f"Could not find targets in {split_name} dataset")
                continue

            # Calculate class distribution
            unique_classes, class_counts = np.unique(targets, return_counts=True)
            total_samples = len(targets)

            logger.info(f"  Total samples: {total_samples:,}")
            logger.info(f"  Class distribution:")

            for class_idx, count in zip(unique_classes, class_counts):
                class_name = (
                    class_names[int(class_idx)]
                    if class_idx < len(class_names)
                    else f"Class {int(class_idx)}"
                )
                percentage = (count / total_samples) * 100
                logger.info(f"    {class_name}: {count:,} ({percentage:.2f}%)")

            # Calculate balance metrics
            if len(class_counts) > 1:
                max_class_count = np.max(class_counts)
                min_class_count = np.min(class_counts)
                imbalance_ratio = max_class_count / min_class_count
                logger.info(f"  Imbalance ratio (max/min): {imbalance_ratio:.2f}")

                # Check if this looks like an undersampled dataset
                if split_name == "train" and imbalance_ratio < 2.0:
                    logger.info(
                        f"  ✓ Training set appears to be undersampled (low imbalance ratio)"
                    )
                elif split_name == "train" and imbalance_ratio > 10.0:
                    logger.warning(
                        f"  ⚠ Training set appears highly imbalanced (high imbalance ratio)"
                    )

        # Compare dataset sizes to verify undersampling
        if "train" in datasets and "validation" in datasets and "test" in datasets:
            train_size = len(datasets["train"])
            val_size = len(datasets["validation"])
            test_size = len(datasets["test"])

            logger.info(f"\nDATASET SIZE COMPARISON:")
            logger.info(f"  Train: {train_size:,}")
            logger.info(
                f"  Validation: {val_size:,} ({val_size/train_size:.1f}x larger)"
            )
            logger.info(f"  Test: {test_size:,} ({test_size/train_size:.1f}x larger)")

            if val_size > train_size * 10 and test_size > train_size * 10:
                logger.info(
                    f"  ✓ Training set is significantly smaller - undersampling was applied"
                )
            else:
                logger.info(
                    f"  ℹ Training set size is similar to other splits - no undersampling detected"
                )

        logger.info("=" * 60)

    def print_detailed_classification_report(
        self, targets, predictions, split_name: str = "test"
    ):
        """Print detailed classification report."""
        logger.info(f"\n{split_name.upper()} SET - DETAILED CLASSIFICATION REPORT:")
        logger.info("=" * 50)

        # Classification report
        report = classification_report(
            targets, predictions, target_names=self.class_names, zero_division=0
        )
        logger.info(f"\n{report}")

        # Confusion matrix
        cm = confusion_matrix(targets, predictions)
        logger.info(f"\nConfusion Matrix:")
        logger.info(f"                Predicted")
        logger.info(f"                Hold   Buy  Sell")
        logger.info(f"Actual   Hold   {cm[0,0]:4d}  {cm[0,1]:4d}  {cm[0,2]:4d}")
        logger.info(f"         Buy    {cm[1,0]:4d}  {cm[1,1]:4d}  {cm[1,2]:4d}")
        logger.info(f"         Sell   {cm[2,0]:4d}  {cm[2,1]:4d}  {cm[2,2]:4d}")

        logger.info("=" * 50)


def evaluate_model(
    model: nn.Module,
    dataset,
    device: str = "cpu",
    split: str = "test",
    batch_size: int = 512,
) -> Dict[str, Any]:
    """Convenience function to evaluate a model without instantiating ModelEvaluator."""
    evaluator = ModelEvaluator(device)
    return evaluator.evaluate_model(model, dataset, split, batch_size)
