# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: MIT-0

import os
import json
import argparse
from pathlib import Path
from datetime import datetime
from typing import List, Dict, Any
import warnings

warnings.filterwarnings("ignore")

import logging
import boto3


# Import from refactored modules
from train import TrainingOrchestrator as BaseTrainingOrchestrator

# Set up logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger("batch_train")


class BatchModelTrainer(BaseTrainingOrchestrator):
    """Extended TrainingOrchestrator with S3 download/upload capabilities for AWS Batch."""

    def __init__(self, config_dict: Dict[str, Any], device: str = None):
        # Don't call super().__init__ yet, we need to set up S3 first

        # S3 configuration
        self.s3_bucket = config_dict.get("s3", {}).get("bucket_name", None)
        self.s3_prefix = config_dict.get("s3", {}).get("prefix", "ml-pipeline")
        self.region = config_dict.get("s3", {}).get("region", "us-east-1")

        # Initialize S3 client
        if self.s3_bucket:
            self.s3_client = boto3.client("s3", region_name=self.region)
            logger.info(f"Initialized S3 client for bucket: {self.s3_bucket}")
        else:
            self.s3_client = None
            logger.warning("No S3 bucket configured")

        # Set up local datasets directory
        self.local_datasets_dir = config_dict.get("training", {}).get(
            "local_datasets_dir", "/tmp/datasets"
        )
        config_dict.setdefault("data", {})[
            "prepared_datasets_dir"
        ] = self.local_datasets_dir

        # Create datasets directory if it doesn't exist
        try:
            Path(self.local_datasets_dir).mkdir(parents=True, exist_ok=True)
            logger.info(f"✓ Created datasets directory: {self.local_datasets_dir}")
        except PermissionError as e:
            logger.error(
                f"Permission denied creating datasets directory: {self.local_datasets_dir}"
            )
            logger.error("Trying fallback directory: /tmp/datasets")
            self.local_datasets_dir = "/tmp/datasets"
            Path(self.local_datasets_dir).mkdir(parents=True, exist_ok=True)
            config_dict.setdefault("data", {})[
                "prepared_datasets_dir"
            ] = self.local_datasets_dir
            logger.info(
                f"✓ Using fallback datasets directory: {self.local_datasets_dir}"
            )

        # Now call parent constructor
        super().__init__(config_dict, device)

        # Override save directory for models
        self.save_dir = config_dict.get("training", {}).get(
            "local_model_save_dir", "/tmp/models"
        )
        try:
            Path(self.save_dir).mkdir(parents=True, exist_ok=True)
            logger.info(f"✓ Created models directory: {self.save_dir}")
        except PermissionError as e:
            logger.error(
                f"Permission denied creating models directory: {self.save_dir}"
            )
            logger.error("Trying fallback directory: /tmp/models")
            self.save_dir = "/tmp/models"
            Path(self.save_dir).mkdir(parents=True, exist_ok=True)
            logger.info(f"✓ Using fallback models directory: {self.save_dir}")

    def _get_s3_datasets_path(
        self, instrument: str, start_date: str, end_date: str
    ) -> str:
        """Generate S3 path for datasets based on instrument and date range."""
        return f"{self.s3_prefix}/datasets/instrument={instrument}/start_date={start_date}/end_date={end_date}/"

    def _get_s3_models_path(
        self, instrument: str, start_date: str, end_date: str
    ) -> str:
        """Generate S3 path for models based on instrument and date range."""
        return f"{self.s3_prefix}/models/instrument={instrument}/start_date={start_date}/end_date={end_date}/"

    def _download_from_s3(self, s3_key: str, local_path: str) -> bool:
        """Download a file from S3."""
        if not self.s3_client or not self.s3_bucket:
            logger.error("S3 not configured")
            return False

        try:
            # Create local directory if it doesn't exist
            Path(local_path).parent.mkdir(parents=True, exist_ok=True)

            logger.info(f"Downloading s3://{self.s3_bucket}/{s3_key} to {local_path}")
            self.s3_client.download_file(self.s3_bucket, s3_key, local_path)
            logger.info(f"✓ Successfully downloaded from S3")
            return True
        except Exception as e:
            logger.error(f"Failed to download from S3: {e}")
            return False

    def _upload_to_s3(self, local_path: str, s3_key: str) -> bool:
        """Upload a file to S3."""
        if not self.s3_client or not self.s3_bucket:
            logger.error("S3 not configured")
            return False

        try:
            logger.info(f"Uploading {local_path} to s3://{self.s3_bucket}/{s3_key}")
            self.s3_client.upload_file(local_path, self.s3_bucket, s3_key)
            logger.info(
                f"✓ Successfully uploaded to S3: s3://{self.s3_bucket}/{s3_key}"
            )
            return True
        except Exception as e:
            logger.error(f"Failed to upload to S3: {e}")
            return False

    def download_datasets_from_s3(
        self, instrument: str, start_date: str, end_date: str
    ) -> bool:
        """Download prepared datasets from S3 with disk space management."""
        logger.info(
            f"Downloading datasets for instrument={instrument}, start_date={start_date}, end_date={end_date}"
        )

        # Check available disk space before starting
        total_space_gb = self._get_total_available_space()
        logger.info(f"Available disk space: {total_space_gb:.1f}GB")

        # Require at least 6GB for datasets (estimate: 2GB each for train/val/test)
        if total_space_gb < 6.0:
            logger.error(
                f"⚠️ Insufficient disk space! Available: {total_space_gb:.1f}GB, Required: ~6GB"
            )
            logger.error("Solutions to increase available space:")
            logger.error(
                "1. Reduce dataset size by using more aggressive undersampling in data prep"
            )
            logger.error("2. Use shorter sequence_length (e.g., 64 instead of 128)")
            logger.error("3. Mount additional EBS volumes in Batch job definition")
            logger.error("4. Use EC2 instances with more local storage")
            logger.error("5. Use date ranges with fewer days of data")
            raise RuntimeError(
                f"Insufficient disk space: {total_space_gb:.1f}GB < 6GB required"
            )

        s3_datasets_path = self._get_s3_datasets_path(instrument, start_date, end_date)

        # Download all datasets
        files_to_download = [
            "train_dataset.pt",
            "validation_dataset.pt",
            "test_dataset.pt",
            "scaler.pkl",
            "preparation_config.json",
        ]

        downloaded_files = []

        for filename in files_to_download:
            s3_key = s3_datasets_path + filename
            local_path = Path(self.local_datasets_dir) / filename

            # Check space before each large download
            if filename.endswith(".pt"):  # Large dataset files
                current_space = self._get_total_available_space()
                if current_space < 2.0:
                    logger.error(
                        f"⚠️ Running out of space before downloading {filename}! Only {current_space:.1f}GB left"
                    )
                    break

            if self._download_from_s3(s3_key, str(local_path)):
                downloaded_files.append(filename)
                logger.info(
                    f"✓ Downloaded {filename} ({self._get_file_size(local_path):.1f} MB)"
                )
            else:
                logger.warning(f"Failed to download {filename} - continuing without it")

        if len(downloaded_files) >= 3:  # At least the dataset files
            logger.info(
                f"✓ Successfully downloaded {len(downloaded_files)} files from S3"
            )
            return True
        else:
            logger.error(
                f"Failed to download sufficient files from S3. Got {len(downloaded_files)} files."
            )
            return False

    def _get_total_available_space(self) -> float:
        """Get total available disk space in GB."""
        import shutil

        total, used, free = shutil.disk_usage(self.local_datasets_dir)
        return free / (1024**3)

    def _check_disk_space(self, required_gb: float = 1.0):
        """Check available disk space and warn if insufficient."""
        import shutil

        # Check space in the target directory
        total, used, free = shutil.disk_usage(self.local_datasets_dir)

        free_gb = free / (1024**3)
        used_gb = used / (1024**3)
        total_gb = total / (1024**3)

        logger.info(
            f"Disk space: {free_gb:.1f}GB free / {total_gb:.1f}GB total (used: {used_gb:.1f}GB)"
        )

        if free_gb < required_gb:
            logger.error(
                f"⚠️ Insufficient disk space! Required: {required_gb:.1f}GB, Available: {free_gb:.1f}GB"
            )
            logger.error("Consider:")
            logger.error("1. Using smaller datasets or better undersampling")
            logger.error("2. Mounting additional storage volumes")
            logger.error("3. Using streaming data loading")
            raise RuntimeError(
                f"Insufficient disk space: {free_gb:.1f}GB < {required_gb:.1f}GB required"
            )

    def _get_file_size(self, file_path: str) -> float:
        """Get file size in MB."""
        try:
            size_bytes = Path(file_path).stat().st_size
            return size_bytes / (1024**2)  # Convert to MB
        except Exception:
            return 0.0

    def upload_models_to_s3(
        self,
        instrument: str,
        start_date: str,
        end_date: str,
        training_results: Dict[str, Any],
    ) -> Dict[str, Any]:
        """Upload trained models to S3."""
        logger.info(
            f"Uploading models for instrument={instrument}, start_date={start_date}, end_date={end_date}"
        )

        s3_models_path = self._get_s3_models_path(instrument, start_date, end_date)
        uploaded_files = []

        # Upload each trained model
        for model_name, result in training_results.items():
            if "model_save_path" in result:
                local_model_path = result["model_save_path"]
                model_filename = Path(local_model_path).name
                s3_model_key = s3_models_path + model_filename

                if self._upload_to_s3(local_model_path, s3_model_key):
                    uploaded_files.append(
                        {
                            "model_name": model_name,
                            "local_path": local_model_path,
                            "s3_path": f"s3://{self.s3_bucket}/{s3_model_key}",
                            "test_accuracy": result.get("test_metrics", {}).get(
                                "accuracy", 0.0
                            ),
                            "test_f1": result.get("test_metrics", {}).get(
                                "f1_macro", 0.0
                            ),
                        }
                    )

        # Create training summary
        training_summary = {
            "pipeline_stage": "model_training",
            "completed_at": datetime.now().isoformat(),
            "instrument": instrument,
            "start_date": start_date,
            "end_date": end_date,
            "s3_bucket": self.s3_bucket,
            "s3_models_path": s3_models_path,
            "trained_models": uploaded_files,
            "training_results": {
                name: {
                    "training_time": result.get("training_time", 0),
                    "best_val_accuracy": result.get("best_val_accuracy", 0.0),
                    "test_accuracy": result.get("test_metrics", {}).get(
                        "accuracy", 0.0
                    ),
                    "test_f1": result.get("test_metrics", {}).get("f1_macro", 0.0),
                }
                for name, result in training_results.items()
            },
        }

        # Save training summary locally
        summary_filename = "training_summary.json"
        summary_path = Path(self.save_dir) / summary_filename
        with open(summary_path, "w") as f:
            json.dump(training_summary, f, indent=2, default=str)

        # Upload training summary to S3
        s3_summary_key = s3_models_path + summary_filename
        if self._upload_to_s3(str(summary_path), s3_summary_key):
            uploaded_files.append(
                {
                    "model_name": "training_summary",
                    "local_path": str(summary_path),
                    "s3_path": f"s3://{self.s3_bucket}/{s3_summary_key}",
                    "type": "summary",
                }
            )

        logger.info(f"✓ Successfully uploaded {len(uploaded_files)} files to S3")
        logger.info(f"✓ Models available at: s3://{self.s3_bucket}/{s3_models_path}")

        return training_summary

    def run_full_training_pipeline(
        self,
        instrument: str,
        start_date: str,
        end_date: str,
        models_to_train: List[str],
    ) -> Dict[str, Any]:
        """Run the complete training pipeline: download → train → upload."""
        logger.info("=== Starting Batch Training Pipeline ===")

        # Step 1: Download datasets from S3
        logger.info("Step 1: Downloading datasets from S3...")
        if not self.download_datasets_from_s3(instrument, start_date, end_date):
            raise RuntimeError("Failed to download datasets from S3")

        # Step 2: Load datasets
        logger.info("Step 2: Loading datasets...")
        self.load_datasets()

        # Step 3: Train models
        logger.info("Step 3: Training models...")
        training_results = {}

        for model_name in models_to_train:
            try:
                logger.info(f"\n{'='*50}")
                logger.info(f"Training {model_name}")
                logger.info(f"{'='*50}")

                # Train model
                training_result = self.train_model(model_name)

                # Evaluate on test set
                test_metrics = self.evaluate_model(training_result["model"], "test")

                # Store results
                training_results[model_name] = {
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

        if not training_results:
            raise RuntimeError("No models were successfully trained!")

        # Step 4: Upload models to S3
        logger.info("Step 4: Uploading models to S3...")
        upload_summary = self.upload_models_to_s3(
            instrument, start_date, end_date, training_results
        )

        logger.info("=== Batch Training Pipeline Complete ===")

        # Final summary
        logger.info(f"\n{'='*60}")
        logger.info("TRAINING SUMMARY")
        logger.info(f"{'='*60}")

        for model_name, result in training_results.items():
            logger.info(f"\n{model_name}:")
            logger.info(f"  Training Time: {result['training_time']:.2f}s")
            logger.info(f"  Validation Accuracy: {result['best_val_accuracy']:.4f}")
            logger.info(f"  Test Accuracy: {result['test_metrics']['accuracy']:.4f}")
            logger.info(f"  Test F1 Score: {result['test_metrics']['f1_macro']:.4f}")

        # Find best model
        best_model = max(
            training_results.items(), key=lambda x: x[1]["test_metrics"]["accuracy"]
        )
        logger.info(
            f"\n🏆 Best model: {best_model[0]} (Test Accuracy: {best_model[1]['test_metrics']['accuracy']:.4f})"
        )

        logger.info(
            f"\n✓ Training completed successfully! Trained {len(training_results)} models."
        )

        return upload_summary


def load_config(config_path: str) -> Dict[str, Any]:
    """Load configuration from local file or S3."""
    if config_path.startswith("s3://"):
        # Clean and parse S3 path - handle malformed paths
        cleaned_path = config_path.replace("s3:///", "s3://").replace("s3://", "")

        # Handle case where bucket name might be missing
        if cleaned_path.startswith("/") or not cleaned_path or "/" not in cleaned_path:
            logger.error(f"Malformed S3 path: {config_path}")
            logger.error("S3 path should be in format: s3://bucket-name/path/to/file")
            raise ValueError(
                f"Invalid S3 path format: {config_path}. Expected: s3://bucket-name/path/to/file"
            )

        parts = cleaned_path.split("/", 1)
        if len(parts) != 2:
            logger.error(f"Cannot parse S3 path: {config_path}")
            raise ValueError(
                f"Invalid S3 path format: {config_path}. Expected: s3://bucket-name/path/to/file"
            )

        bucket, key = parts

        if not bucket or not key:
            logger.error(f"Missing bucket name or key in S3 path: {config_path}")
            raise ValueError(
                f"Invalid S3 path format: {config_path}. Both bucket and key must be specified"
            )

        logger.info(f"Loading config from S3: s3://{bucket}/{key}")

        # Create S3 client with timeout configuration
        try:
            # Use a fresh session with timeout configuration
            import boto3.session
            from botocore.config import Config

            # Configure timeouts to prevent hanging
            config = Config(
                read_timeout=60,
                connect_timeout=30,
                retries={"max_attempts": 3, "mode": "standard"},
            )

            session = boto3.session.Session()
            s3_client = session.client("s3", config=config)

            logger.info(f"Attempting to download config from s3://{bucket}/{key}...")
            response = s3_client.get_object(Bucket=bucket, Key=key)
            config_content = response["Body"].read().decode("utf-8")
            config_dict = json.loads(config_content)
            logger.info(f"✓ Successfully loaded config from S3: s3://{bucket}/{key}")
            return config_dict

        except Exception as e:
            logger.error(f"Failed to load config from S3: {e}")
            logger.error(f"Bucket: {bucket}, Key: {key}")
            logger.error("Check that:")
            logger.error("1. The S3 bucket exists and you have access")
            logger.error("2. The config file exists at the specified path")
            logger.error("3. Your VPC has proper S3 endpoint connectivity")
            logger.error("4. Your IAM role has s3:GetObject permission")
            raise FileNotFoundError(
                f"Configuration file not found or inaccessible: {config_path}. Error: {str(e)}"
            )
    else:
        # Load from local file
        if not os.path.exists(config_path):
            raise FileNotFoundError(f"Configuration file not found: {config_path}")

        with open(config_path, "r") as f:
            config_dict = json.load(f)
        logger.info(f"✓ Loaded configuration from local file: {config_path}")
        return config_dict


def main():
    """Main training function for AWS Batch."""
    parser = argparse.ArgumentParser(
        description="Batch Model Training with S3 Integration"
    )
    parser.add_argument(
        "--config",
        type=str,
        required=True,
        help="Configuration file (local path or s3://bucket/key)",
    )
    parser.add_argument(
        "--instrument", type=str, help="Instrument ID (overrides config)"
    )
    parser.add_argument(
        "--start-date", type=str, help="Start date YYYY-MM-DD (overrides config)"
    )
    parser.add_argument(
        "--end-date", type=str, help="End date YYYY-MM-DD (overrides config)"
    )
    parser.add_argument(
        "--models", nargs="+", default=["CNN_LSTM"], help="Models to train"
    )
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
        config_dict = load_config(args.config)

        # Extract parameters from config (with command line overrides)
        instrument = args.instrument or config_dict.get("data", {}).get(
            "instrument_filter"
        )
        start_date = args.start_date or config_dict.get("data", {}).get("start_date")
        end_date = args.end_date or config_dict.get("data", {}).get("end_date")

        # Validate required parameters
        if not instrument:
            raise ValueError(
                "Instrument must be specified either in config file (data.instrument_filter) or via --instrument argument"
            )
        if not start_date:
            raise ValueError(
                "Start date must be specified either in config file (data.start_date) or via --start-date argument"
            )
        if not end_date:
            raise ValueError(
                "End date must be specified either in config file (data.end_date) or via --end-date argument"
            )

        # Override config with command line arguments
        if args.epochs:
            config_dict.setdefault("training", {})["total_epochs"] = args.epochs
        if args.batch_size:
            config_dict.setdefault("training", {})["batch_size"] = args.batch_size
        if args.lr:
            config_dict.setdefault("training", {})["learning_rate"] = args.lr

        # Initialize trainer
        trainer = BatchModelTrainer(config_dict, device=args.device)

        logger.info(f"\n{'='*60}")
        logger.info(f"BATCH TRAINING PIPELINE")
        logger.info(f"{'='*60}")
        logger.info(f"Instrument: {instrument}")
        logger.info(f"Date Range: {start_date} to {end_date}")
        logger.info(f"Models: {args.models}")
        logger.info(f"S3 Bucket: {trainer.s3_bucket}")
        logger.info(f"{'='*60}")

        # Run full training pipeline
        upload_summary = trainer.run_full_training_pipeline(
            instrument=instrument,
            start_date=start_date,
            end_date=end_date,
            models_to_train=args.models,
        )

        logger.info(f"\n✓ Batch training pipeline completed successfully!")
        logger.info(
            f"✓ Results uploaded to S3: s3://{upload_summary['s3_bucket']}/{upload_summary['s3_models_path']}"
        )

        # Save final summary locally for debugging
        final_summary_path = Path(trainer.save_dir) / "final_summary.json"
        with open(final_summary_path, "w") as f:
            json.dump(upload_summary, f, indent=2, default=str)
        logger.info(f"✓ Final summary saved to: {final_summary_path}")

    except Exception as e:
        logger.error(f"❌ Batch training failed: {e}")
        import traceback

        traceback.print_exc()
        return 1

    return 0


if __name__ == "__main__":
    exit(main())
