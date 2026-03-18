# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: MIT-0

"""
Model training module with optimized training loops and utilities.
"""

import os
import time
import torch
import torch.nn as nn
import torch.optim as optim
from torch.utils.data import DataLoader
from sklearn.preprocessing import MinMaxScaler
from pathlib import Path
from typing import Dict, Any
import logging

# Fix import paths to work from any directory
import sys
import os

# Add the current directory to Python path
current_dir = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, current_dir)

from model_factory import ModelFactory, get_model_forward_pass

logger = logging.getLogger(__name__)


class ModelTrainer:
    """Handles model training with optimized settings and best practices."""

    def __init__(self, config_dict: Dict[str, Any], device: str = None):
        self.config_dict = config_dict

        # Set device
        if device is None:
            self.device = torch.device("cuda" if torch.cuda.is_available() else "cpu")
        else:
            self.device = torch.device(device)

        logger.info(f"Using device: {self.device}")

        # Training parameters
        self.batch_size = int(config_dict.get("training", {}).get("batch_size", 32))
        self.epochs = int(config_dict.get("training", {}).get("total_epochs", 10))
        self.learning_rate = float(
            config_dict.get("training", {}).get("learning_rate", 0.001)
        )
        self.save_dir = config_dict.get("training", {}).get("model_save_dir", "models")

        # Create save directory
        Path(self.save_dir).mkdir(parents=True, exist_ok=True)

        # Dataset parameters
        self.datasets_dir = config_dict.get("data", {}).get(
            "prepared_datasets_dir", "data/prepared_datasets"
        )

        # Model parameters
        self.hidden_size = int(config_dict.get("model", {}).get("hidden_size", 128))
        self.num_layers = int(config_dict.get("model", {}).get("num_layers", 2))
        self.num_classes = 3  # Hold, Buy, Sell

        # Training state
        self.datasets = None
        self.num_features = None
        self.sequence_length = None
        self.model_factory = None

    def setup(self, datasets: Dict[str, Any], num_features: int, sequence_length: int):
        """Setup trainer with datasets and model parameters."""
        self.datasets = datasets
        self.num_features = num_features
        self.sequence_length = sequence_length

        # Initialize model factory
        self.model_factory = ModelFactory(
            num_features, sequence_length, self.num_classes
        )

        logger.info(
            f"✓ Trainer setup complete - {num_features} features, sequence length {sequence_length}"
        )

    def train_model(self, model_name: str) -> Dict[str, Any]:
        """Train a single model with optimized settings."""
        logger.info(f"Training {model_name}...")

        if self.model_factory is None:
            raise ValueError("Trainer not setup. Call setup() first.")

        # Get model
        model_config = {"hidden_size": self.hidden_size, "num_layers": self.num_layers}
        model = self.model_factory.create_model(model_name, **model_config).to(
            self.device
        )

        # Setup training components
        train_loader, validation_loader = self._create_data_loaders()
        optimizer, criterion, scheduler = self._setup_training_components(model)
        scaler = self._load_scaler()

        # Optimization: Compile model for faster inference (PyTorch 2.0+)
        try:
            model = torch.compile(model, mode="reduce-overhead")
            logger.info("✓ Model compiled for faster inference")
        except Exception as e:
            logger.info(f"Model compilation not available: {e}")

        # Mixed precision setup
        use_amp = torch.cuda.is_available()
        torch.cuda.amp.GradScaler() if use_amp else None
        if use_amp:
            logger.info("✓ Using mixed precision for faster training")

        # Training loop
        training_result = self._training_loop(
            model,
            train_loader,
            validation_loader,
            optimizer,
            criterion,
            scheduler,
            scaler,
            model_name,
        )

        return training_result

    def _create_data_loaders(self) -> tuple:
        """Create optimized data loaders for training and validation."""
        # Check if dataset tensors are on CPU or GPU
        sample_data = self.datasets["train"][0]
        data_on_cuda = (
            sample_data[0].is_cuda if hasattr(sample_data[0], "is_cuda") else False
        )

        # Training loader
        train_loader = DataLoader(
            self.datasets["train"],
            batch_size=self.batch_size,
            shuffle=True,
            pin_memory=False,  # Disable pin_memory since we handle CPU/GPU transfers manually
            num_workers=0,  # Keep 0 for training to avoid CUDA errors
        )

        # Use larger batch size for validation (no gradients needed)
        val_batch_size = min(
            self.batch_size * 4, 512
        )  # 4x larger for faster validation
        validation_loader = DataLoader(
            self.datasets["validation"],
            batch_size=val_batch_size,
            shuffle=False,
            pin_memory=False,  # Disable pin_memory to avoid CUDA tensor pinning errors
            num_workers=0,  # Disable workers to avoid CUDA multiprocessing issues
            persistent_workers=False,
        )

        logger.info(f"Dataset tensors are on: {'CUDA' if data_on_cuda else 'CPU'}")
        logger.info(
            f"Training batch size: {self.batch_size}, Validation batch size: {val_batch_size}"
        )

        return train_loader, validation_loader

    def _setup_training_components(self, model):
        """Setup optimizer, criterion, and scheduler."""
        optimizer = optim.AdamW(
            model.parameters(), lr=self.learning_rate, weight_decay=1e-4
        )
        criterion = nn.CrossEntropyLoss()
        scheduler = optim.lr_scheduler.ReduceLROnPlateau(
            optimizer, "min", patience=3, factor=0.5
        )

        return optimizer, criterion, scheduler

    def _load_scaler(self) -> MinMaxScaler:
        """Load the scaler from data preparation."""
        logger.info("Loading scaler from data preparation...")

        # Look for scaler file in the datasets directory
        scaler_path = os.path.join(self.datasets_dir, "scaler.pkl")

        if os.path.exists(scaler_path):
            import pickle

            with open(scaler_path, "rb") as f:
                scaler = pickle.load(f)
            logger.info(f"✓ Loaded scaler from {scaler_path}")
            return scaler
        else:
            logger.warning(
                f"⚠️ Scaler file not found at {scaler_path}, creating fallback scaler"
            )
            # Fallback to identity scaler since data is already scaled
            scaler = MinMaxScaler()
            dummy_data = [[0.0] * self.num_features, [1.0] * self.num_features]
            scaler.fit(dummy_data)
            return scaler

    def _training_loop(
        self,
        model,
        train_loader,
        validation_loader,
        optimizer,
        criterion,
        scheduler,
        scaler,
        model_name,
    ) -> Dict[str, Any]:
        """Execute the main training loop."""
        # Training state
        best_loss = float("inf")
        best_accuracy = 0.0
        training_history = {"train_loss": [], "val_loss": [], "val_accuracy": []}

        train_start_time = time.time()

        for epoch in range(self.epochs):
            epoch_start = time.time()

            # Training phase
            avg_train_loss = self._train_epoch(
                model, train_loader, optimizer, criterion, epoch
            )

            # Validation phase
            avg_val_loss, val_accuracy = self._validate_epoch(
                model, validation_loader, criterion
            )

            # Update learning rate
            scheduler.step(avg_val_loss)

            # Save training history
            training_history["train_loss"].append(avg_train_loss)
            training_history["val_loss"].append(avg_val_loss)
            training_history["val_accuracy"].append(val_accuracy)

            epoch_time = time.time() - epoch_start

            logger.info(
                f"  Epoch {epoch+1}/{self.epochs}: "
                f"Train Loss: {avg_train_loss:.4f}, "
                f"Val Loss: {avg_val_loss:.4f}, "
                f"Val Acc: {val_accuracy:.4f}, "
                f"Time: {epoch_time:.2f}s"
            )

            # Save best model
            if val_accuracy > best_accuracy:
                best_accuracy = val_accuracy
                best_loss = avg_val_loss
                self._save_best_model(
                    model,
                    optimizer,
                    scaler,
                    epoch,
                    best_accuracy,
                    best_loss,
                    training_history,
                    model_name,
                )

        total_train_time = time.time() - train_start_time

        # Load best model for final evaluation
        best_model_path = os.path.join(self.save_dir, f"{model_name}_best.pt")
        if os.path.exists(best_model_path):
            checkpoint = torch.load(best_model_path, weights_only=False)
            model.load_state_dict(checkpoint["model_state_dict"])

        return {
            "model": model,
            "training_time": total_train_time,
            "best_val_loss": best_loss,
            "best_val_accuracy": best_accuracy,
            "training_history": training_history,
            "model_save_path": best_model_path,
        }

    def _train_epoch(self, model, train_loader, optimizer, criterion, epoch) -> float:
        """Execute one training epoch."""
        model.train()
        train_loss = 0.0
        num_batches = 0
        targets_fixed = False  # Track if we've logged the tensor fix

        for batch_idx, (inputs, targets) in enumerate(train_loader):
            inputs = inputs.to(self.device)
            targets = targets.to(self.device)

            # Fix targets shape if needed (with minimal logging)
            if len(targets.shape) > 1:
                if not targets_fixed:
                    logger.info(
                        f"Auto-fixing tensor shapes: {targets.shape} → 1D for CrossEntropyLoss"
                    )
                    targets_fixed = True

                if targets.shape[1] == 1:
                    targets = targets.squeeze(1)
                elif targets.shape[1] > 1:
                    targets = torch.argmax(targets, dim=1)

            optimizer.zero_grad()

            # Use model factory forward pass handler
            outputs = get_model_forward_pass(model, inputs)

            loss = criterion(outputs, targets)
            loss.backward()

            # Gradient clipping
            torch.nn.utils.clip_grad_norm_(model.parameters(), max_norm=1.0)

            optimizer.step()

            train_loss += loss.item()
            num_batches += 1

            if batch_idx % 100 == 0:
                logger.info(
                    f"  Epoch {epoch+1}/{self.epochs}, Batch {batch_idx}/{len(train_loader)}, Loss: {loss.item():.4f}"
                )

        return train_loss / num_batches

    def _validate_epoch(self, model, validation_loader, criterion) -> tuple:
        """Execute one validation epoch."""
        model.eval()
        val_loss = 0.0
        val_correct = 0
        val_total = 0

        with torch.no_grad():
            for inputs, targets in validation_loader:
                inputs = inputs.to(self.device)
                targets = targets.to(self.device)

                # Fix targets shape if needed (same as in training)
                if len(targets.shape) > 1:
                    if targets.shape[1] == 1:
                        targets = targets.squeeze(1)
                    elif targets.shape[1] > 1:
                        targets = torch.argmax(targets, dim=1)

                # Use model factory forward pass handler
                outputs = get_model_forward_pass(model, inputs)

                loss = criterion(outputs, targets)
                val_loss += loss.item()

                _, predicted = torch.max(outputs.data, 1)
                val_total += targets.size(0)
                val_correct += (predicted == targets).sum().item()

        avg_val_loss = val_loss / len(validation_loader)
        val_accuracy = val_correct / val_total

        return avg_val_loss, val_accuracy

    def _save_best_model(
        self,
        model,
        optimizer,
        scaler,
        epoch,
        best_accuracy,
        best_loss,
        training_history,
        model_name,
    ) -> str:
        """Save the best model checkpoint."""
        model_save_path = os.path.join(self.save_dir, f"{model_name}_best.pt")

        torch.save(
            {
                "model_state_dict": model.state_dict(),
                "optimizer_state_dict": optimizer.state_dict(),
                "scaler": scaler,
                "epoch": epoch,
                "best_accuracy": best_accuracy,
                "best_loss": best_loss,
                "training_history": training_history,
                "model_config": {
                    "model_name": model_name,
                    "num_features": self.num_features,
                    "sequence_length": self.sequence_length,
                    "hidden_size": self.hidden_size,
                    "num_layers": self.num_layers,
                    "num_classes": self.num_classes,
                },
            },
            model_save_path,
        )

        logger.info(
            f"  ✓ Best model saved: {model_save_path} (acc: {best_accuracy:.4f})"
        )
        return model_save_path


def train_model(
    model_name: str,
    datasets: Dict[str, Any],
    num_features: int,
    sequence_length: int,
    config_dict: Dict[str, Any],
    device: str = None,
) -> Dict[str, Any]:
    """Convenience function to train a model without instantiating ModelTrainer."""
    trainer = ModelTrainer(config_dict, device)
    trainer.setup(datasets, num_features, sequence_length)
    return trainer.train_model(model_name)
