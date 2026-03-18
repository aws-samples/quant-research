# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: MIT-0

import os
import json
import argparse
from typing import Dict, Any
import warnings

warnings.filterwarnings("ignore")

import torch
import torch.nn as nn
import logging

# Fix import paths to work from any directory
import sys
import os

# Add the current directory and parent directory to Python path
current_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir = os.path.dirname(current_dir)
sys.path.insert(0, current_dir)  # Add src directory
sys.path.insert(0, parent_dir)  # Add order_flow directory

# Set up logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger("streamlined_train")

# Now try imports with proper fallback
try:
    from polars_dataprovider import PolarsDataProvider
    from trainer import ModelTrainer
    from evaluator import ModelEvaluator
except ImportError as e:
    logger.error(f"Import error: {e}")
    logger.error(f"Current working directory: {os.getcwd()}")
    logger.error(f"Script directory: {current_dir}")
    logger.error(f"Python path: {sys.path[:5]}")
    raise


class TrainingOrchestrator:
    """Training orchestrator that uses pre-prepared datasets."""

    def __init__(self, config_dict: Dict[str, Any], device: str = None):
        self.config_dict = config_dict

        # Set device
        if device is None:
            self.device = torch.device("cuda" if torch.cuda.is_available() else "cpu")
        else:
            self.device = torch.device(device)

        logger.info(f"Using device: {self.device}")

        # Dataset parameters
        self.datasets_dir = config_dict.get("data", {}).get(
            "prepared_datasets_dir", "data/prepared_datasets"
        )

        # Initialize components
        self.trainer = ModelTrainer(config_dict, str(self.device))
        self.evaluator = ModelEvaluator(str(self.device))

        # Dataset state
        self.datasets = None
        self.num_features = None
        self.sequence_length = None

    def load_datasets(self):
        """Load pre-prepared datasets from disk."""
        logger.info(f"Loading pre-prepared datasets from {self.datasets_dir}...")

        try:
            self.datasets = PolarsDataProvider.load_prepared_datasets(
                datasets_dir=self.datasets_dir, device=self.device
            )

            # Get dataset information
            train_dataset = self.datasets["train"]

            # Handle different attribute names for sequences
            if hasattr(train_dataset, "X"):
                sequences = train_dataset.X
            elif hasattr(train_dataset, "sequences"):
                sequences = train_dataset.sequences
            else:
                raise AttributeError("Could not find sequences in train dataset")

            self.num_features = sequences.shape[2]
            self.sequence_length = sequences.shape[1]

            logger.info(
                f"✓ Loaded datasets with {self.num_features} features, sequence length {self.sequence_length}"
            )
            logger.info(f"  Train: {len(self.datasets['train']):,} samples")
            logger.info(f"  Validation: {len(self.datasets['validation']):,} samples")
            logger.info(f"  Test: {len(self.datasets['test']):,} samples")

            # Show class distribution statistics to verify undersampling
            self.evaluator.show_class_distributions(self.datasets)

            # Setup trainer with loaded datasets
            self.trainer.setup(self.datasets, self.num_features, self.sequence_length)

        except Exception as e:
            logger.error(f"Failed to load datasets: {e}")
            raise RuntimeError(
                f"Could not load datasets from {self.datasets_dir}. "
                f"Please run the data preparation pipeline first using polars_dataprovider.py"
            )

    def train_model(self, model_name: str) -> Dict[str, Any]:
        """Train a single model using the modular trainer."""
        return self.trainer.train_model(model_name)

    def evaluate_model(self, model: nn.Module, split: str = "test") -> Dict[str, Any]:
        """Evaluate model using the modular evaluator."""
        dataset = self.datasets[split]
        batch_size = min(
            self.trainer.batch_size * 4, 512
        )  # Use larger batch size for evaluation
        return self.evaluator.evaluate_model(model, dataset, split, batch_size)


def main():
    """Main training function."""
    parser = argparse.ArgumentParser(
        description="Model Training with Pre-prepared Datasets"
    )
    parser.add_argument(
        "--config",
        type=str,
        default="configs/date_filtering_config.json",
        help="Configuration file",
    )
    parser.add_argument(
        "--datasets-dir",
        type=str,
        default="data/prepared_datasets",
        help="Directory containing prepared datasets",
    )
    parser.add_argument(
        "--model",
        type=str,
        choices=["RNN", "CNN_LSTM", "TFT", "OrderFlow_Transformer"],
        default="RNN",
        help="Model to train",
    )
    parser.add_argument("--models", nargs="+", help="Multiple models to train")
    parser.add_argument(
        "--device", type=str, choices=["cpu", "cuda"], help="Device to use"
    )
    parser.add_argument(
        "--epochs", type=int, help="Number of epochs (overrides config)"
    )
    parser.add_argument("--batch-size", type=int, help="Batch size (overrides config)")
    parser.add_argument("--lr", type=float, help="Learning rate (overrides config)")

    args = parser.parse_args()

    try:
        # Load configuration
        logger.info(f"Loading configuration from {args.config}...")

        if not os.path.exists(args.config):
            logger.warning(
                f"Config file {args.config} not found, using default configuration"
            )
            config_dict = {
                "data": {"prepared_datasets_dir": args.datasets_dir},
                "model": {"hidden_size": 128, "num_layers": 2},
                "training": {
                    "batch_size": 32,
                    "total_epochs": 10,
                    "learning_rate": 0.001,
                    "model_save_dir": "models",
                },
            }
        else:
            with open(args.config, "r") as f:
                config_dict = json.load(f)
            logger.info(f"✓ Loaded configuration from {args.config}")

        # Override config with command line arguments
        if args.datasets_dir:
            config_dict.setdefault("data", {})[
                "prepared_datasets_dir"
            ] = args.datasets_dir
        if args.epochs:
            config_dict.setdefault("training", {})["total_epochs"] = args.epochs
        if args.batch_size:
            config_dict.setdefault("training", {})["batch_size"] = args.batch_size
        if args.lr:
            config_dict.setdefault("training", {})["learning_rate"] = args.lr

        # Initialize trainer
        trainer = TrainingOrchestrator(config_dict, device=args.device)

        # Load datasets
        trainer.load_datasets()

        # Determine which models to train
        if args.models:
            models_to_train = args.models
        else:
            models_to_train = [args.model]

        logger.info(f"Training models: {models_to_train}")

        # Train models
        results = {}
        for model_name in models_to_train:
            try:
                logger.info(f"\n{'='*50}")
                logger.info(f"Training {model_name}")
                logger.info(f"{'='*50}")

                # Train model
                training_result = trainer.train_model(model_name)

                # Evaluate on test set
                test_metrics = trainer.evaluate_model(training_result["model"], "test")

                # Store results
                results[model_name] = {
                    "training_time": training_result["training_time"],
                    "best_val_accuracy": training_result["best_val_accuracy"],
                    "best_val_loss": training_result["best_val_loss"],
                    "model_save_path": training_result["model_save_path"],
                    "test_metrics": test_metrics,
                }

                logger.info(f"✓ {model_name} completed successfully:")
                logger.info(f"  Training time: {training_result['training_time']:.2f}s")
                logger.info(
                    f"  Best validation accuracy: {training_result['best_val_accuracy']:.4f}"
                )
                logger.info(f"  Test accuracy: {test_metrics['accuracy']:.4f}")
                logger.info(f"  Test F1 score: {test_metrics['f1_macro']:.4f}")

            except Exception as e:
                logger.error(f"✗ Failed to train {model_name}: {str(e)}")
                import traceback

                logger.error(traceback.format_exc())

        # Print final summary
        if results:
            logger.info(f"\n{'='*60}")
            logger.info("TRAINING SUMMARY")
            logger.info(f"{'='*60}")

            for model_name, result in results.items():
                logger.info(f"\n{model_name}:")
                logger.info(f"  Training Time: {result['training_time']:.2f}s")
                logger.info(f"  Validation Accuracy: {result['best_val_accuracy']:.4f}")
                logger.info(
                    f"  Test Accuracy: {result['test_metrics']['accuracy']:.4f}"
                )
                logger.info(
                    f"  Test F1 Score: {result['test_metrics']['f1_macro']:.4f}"
                )
                logger.info(f"  Model saved: {result['model_save_path']}")

            # Find best model
            best_model = max(
                results.items(), key=lambda x: x[1]["test_metrics"]["accuracy"]
            )
            logger.info(
                f"\n🏆 Best model: {best_model[0]} (Test Accuracy: {best_model[1]['test_metrics']['accuracy']:.4f})"
            )

            logger.info(
                f"\n✓ Training completed successfully! Trained {len(results)} models."
            )
        else:
            logger.error("❌ No models were successfully trained!")
            return 1

    except Exception as e:
        logger.error(f"❌ Training failed: {e}")
        import traceback

        traceback.print_exc()
        return 1

    return 0


if __name__ == "__main__":
    exit(main())
