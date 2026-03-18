# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: MIT-0

import os
import json
import pickle
import torch
from sklearn.preprocessing import MinMaxScaler
from typing import Dict, Any
import logging
from pathlib import Path
from datetime import datetime

# Import the classes from refactored modules
from polars_dataprovider import PolarsDataProvider as BasePolarsDataProvider
from datasets import OptimizedSequenceDataset
from utils import log_memory_usage, force_garbage_collection

# Set up logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger("batch_polars_dataprovider")


class BatchPolarsDataProvider(BasePolarsDataProvider):
    """Extended PolarsDataProvider with S3 upload capabilities for AWS Batch."""

    def __init__(self, device, config_dict):
        region = config_dict.get("s3", {}).get("region", "us-east-1")
        super().__init__(device, config_dict, region=region)

        # S3 configuration
        self.s3_bucket = config_dict.get("s3", {}).get("bucket_name", None)
        self.s3_prefix = config_dict.get("s3", {}).get("prefix", "ml-pipeline")

        # Initialize S3 client with error handling
        if self.s3_bucket:
            try:
                # Use explicit session to avoid library conflicts
                import boto3.session

                session = boto3.session.Session()
                self.s3_client = session.client("s3", region_name=self.region)
                logger.info(f"Initialized S3 client for bucket: {self.s3_bucket}")
            except Exception as e:
                logger.warning(f"Failed to initialize S3 client: {e}")
                self.s3_client = None
        else:
            self.s3_client = None
            logger.warning("No S3 bucket configured - will only save locally")

    def _get_s3_path(self, dataset_type: str = "datasets") -> str:
        """Generate structured S3 path based on instrument and date range."""
        # Extract instrument from config
        instrument = "unknown"
        if self.instrument_filter:
            if isinstance(self.instrument_filter, list):
                instrument = "_".join(self.instrument_filter)
            else:
                instrument = str(self.instrument_filter)

        # Extract date range
        start_date = self.start_date or "unknown"
        end_date = self.end_date or "unknown"

        # Create structured path: prefix/dataset_type/instrument=X/start_date=Y/end_date=Z/
        s3_path = f"{self.s3_prefix}/{dataset_type}/instrument={instrument}/start_date={start_date}/end_date={end_date}/"

        return s3_path

    def _upload_to_s3(self, local_path: str, s3_key: str) -> bool:
        """Upload a file to S3."""
        if not self.s3_client or not self.s3_bucket:
            logger.warning("S3 not configured - skipping upload")
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

    def _save_datasets_with_s3(
        self, datasets: Dict[str, OptimizedSequenceDataset], output_dir: str
    ):
        """Save prepared datasets locally and upload to S3 with structured paths."""
        logger.info(f"Saving datasets to {output_dir} and uploading to S3...")

        # Create output directory
        Path(output_dir).mkdir(parents=True, exist_ok=True)

        # Get S3 path for datasets
        s3_datasets_path = self._get_s3_path("datasets")

        uploaded_files = []

        # Save each dataset
        for split_name, dataset in datasets.items():
            dataset_filename = f"{split_name}_dataset.pt"
            dataset_path = Path(output_dir) / dataset_filename

            # Handle different dataset types (streaming vs pre-computed)
            sequences = None
            targets = None
            num_features = None

            # Check for pre-computed datasets first
            if hasattr(dataset, "X") and dataset.X is not None:
                sequences = dataset.X
                targets = dataset.y
                num_features = sequences.shape[2]
                logger.info(f"Processing pre-computed dataset: {split_name}")
            elif hasattr(dataset, "sequences") and dataset.sequences is not None:
                sequences = dataset.sequences
                targets = dataset.targets
                num_features = sequences.shape[2]
                logger.info(f"Processing pre-computed dataset: {split_name}")
            else:
                # Handle sequence datasets - materialize them after undersampling (much smaller now)
                logger.info(
                    f"Materializing undersampled sequence dataset: {split_name} ({len(dataset):,} samples)"
                )

                # Materialize the undersampled dataset by extracting all sequences and targets
                sequences_list = []
                targets_list = []

                for i in range(len(dataset)):
                    seq, target = dataset[i]
                    sequences_list.append(seq.cpu())
                    targets_list.append(target.cpu())

                # Stack into tensors
                sequences = torch.stack(sequences_list)
                targets = torch.stack(targets_list)
                num_features = sequences.shape[2]

                logger.info(
                    f"✓ Materialized {split_name} dataset: {sequences.shape} sequences, {targets.shape} targets"
                )

                # Clear the lists to free memory
                del sequences_list, targets_list
                force_garbage_collection()

            # Save as PyTorch tensors locally
            torch.save(
                {
                    "sequences": sequences,
                    "targets": targets,
                    "metadata": {
                        "split": split_name,
                        "num_samples": len(dataset),
                        "sequence_length": self.sequence_length,
                        "num_features": num_features,
                        "created_at": datetime.now().isoformat(),
                        "undersampling_applied": self.apply_undersampling,
                        "undersampling_strategy": (
                            self.undersampling_strategy
                            if self.apply_undersampling
                            else None
                        ),
                        "instrument": self.instrument_filter,
                        "start_date": self.start_date,
                        "end_date": self.end_date,
                    },
                },
                dataset_path,
            )

            logger.info(
                f"✓ Saved {split_name} dataset locally: {dataset_path} ({len(dataset):,} samples)"
            )

            # Upload to S3
            s3_key = s3_datasets_path + dataset_filename
            if self._upload_to_s3(str(dataset_path), s3_key):
                uploaded_files.append(f"s3://{self.s3_bucket}/{s3_key}")

        # Save configuration for reproducibility
        config_filename = "preparation_config.json"
        config_path = Path(output_dir) / config_filename
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
            "instrument_filter": self.instrument_filter,
            "start_date": self.start_date,
            "end_date": self.end_date,
            "region": self.region,
            "s3_bucket": self.s3_bucket,
            "s3_datasets_path": s3_datasets_path,
            "uploaded_files": uploaded_files,
        }

        with open(config_path, "w") as f:
            json.dump(config_data, f, indent=2)

        logger.info(f"✓ Saved preparation config: {config_path}")

        # Upload config to S3
        s3_config_key = s3_datasets_path + config_filename
        if self._upload_to_s3(str(config_path), s3_config_key):
            uploaded_files.append(f"s3://{self.s3_bucket}/{s3_config_key}")

        # Save the scaler that was used for feature scaling
        if self.scaler is not None:
            scaler_filename = "scaler.pkl"
            scaler_path = Path(output_dir) / scaler_filename
            with open(scaler_path, "wb") as f:
                pickle.dump(self.scaler, f)
            logger.info(f"✓ Saved MinMaxScaler to: {scaler_path}")

            # Upload scaler to S3
            s3_scaler_key = s3_datasets_path + scaler_filename
            if self._upload_to_s3(str(scaler_path), s3_scaler_key):
                uploaded_files.append(f"s3://{self.s3_bucket}/{s3_scaler_key}")
        else:
            logger.warning("⚠️ No scaler was created during data preparation")

        # Create a summary file with S3 locations
        summary_filename = "s3_summary.json"
        summary_path = Path(output_dir) / summary_filename
        summary_data = {
            "pipeline_stage": "data_preparation",
            "completed_at": datetime.now().isoformat(),
            "local_directory": str(output_dir),
            "s3_bucket": self.s3_bucket,
            "s3_datasets_path": s3_datasets_path,
            "uploaded_files": uploaded_files,
            "instrument": self.instrument_filter,
            "start_date": self.start_date,
            "end_date": self.end_date,
            "dataset_splits": {
                split_name: len(dataset) for split_name, dataset in datasets.items()
            },
        }

        with open(summary_path, "w") as f:
            json.dump(summary_data, f, indent=2)

        # Upload summary to S3
        s3_summary_key = s3_datasets_path + summary_filename
        self._upload_to_s3(str(summary_path), s3_summary_key)

        logger.info(f"✓ All datasets saved locally to: {output_dir}")
        logger.info(
            f"✓ All datasets uploaded to S3 path: s3://{self.s3_bucket}/{s3_datasets_path}"
        )
        logger.info(f"✓ Uploaded {len(uploaded_files)} files to S3")

        return summary_data

    def load_and_prepare_data_with_s3(
        self, save_datasets: bool = True, output_dir: str = "data/prepared_datasets"
    ) -> Dict[str, Any]:
        """Complete data preparation pipeline with S3 upload."""
        logger.info("=== Starting Batch Polars-based Data Preparation Pipeline ===")
        log_memory_usage("at pipeline start")

        # Run the original pipeline
        datasets = self.load_and_prepare_data(
            save_datasets=False, output_dir=output_dir
        )
        log_memory_usage("after data preparation")
        force_garbage_collection()

        # Convert Polars scaling params to scikit-learn scaler for compatibility
        if hasattr(self, "scaling_params") and self.scaling_params:
            logger.info(
                "Converting Polars scaling parameters to scikit-learn MinMaxScaler..."
            )
            self.scaler = self._create_sklearn_scaler_from_polars_params(
                self.scaling_params
            )
            logger.info(
                "✓ Created compatible scikit-learn scaler for training pipeline"
            )
        else:
            logger.warning("⚠️ No scaling parameters found from data preparation")
            self.scaler = None

        # Save with S3 upload
        if save_datasets:
            logger.info("Step 6: Saving datasets locally and uploading to S3...")
            s3_summary = self._save_datasets_with_s3(datasets, output_dir)

            # Add S3 summary to return data
            datasets["s3_summary"] = s3_summary
            log_memory_usage("after S3 upload")
            force_garbage_collection()

        logger.info("=== Batch Data Preparation Pipeline Complete ===")
        log_memory_usage("at pipeline completion")
        return datasets

    def _create_sklearn_scaler_from_polars_params(
        self, scaling_params: dict
    ) -> MinMaxScaler:
        """Convert Polars scaling parameters to a scikit-learn MinMaxScaler."""
        scaler = MinMaxScaler()

        # Extract feature names and create dummy data to fit the scaler
        feature_names = list(scaling_params.keys())
        num_features = len(feature_names)

        # Create dummy data with min and max values for each feature
        dummy_data = []
        for feature in feature_names:
            params = scaling_params[feature]
            min_val = params["min"]
            max_val = params["max"]
            dummy_data.append([min_val, max_val])

        # Transpose to get proper shape: [[min1, min2, ...], [max1, max2, ...]]
        dummy_data = list(map(list, zip(*dummy_data)))

        # Fit the scaler with the min/max data
        scaler.fit(dummy_data)

        logger.info(f"✓ Created MinMaxScaler for {num_features} features")
        return scaler


def prepare_data_pipeline_with_s3(
    config_dict: Dict[str, Any],
    save_datasets: bool = True,
    output_dir: str = "data/prepared_datasets",
) -> Dict[str, Any]:
    """Standalone function to run the complete data preparation pipeline with S3 upload."""
    logger.info("=== Starting Batch Standalone Data Preparation Pipeline ===")
    log_memory_usage("at standalone pipeline start")

    # Set device
    device = torch.device("cuda" if torch.cuda.is_available() else "cpu")
    logger.info(f"Using device: {device}")

    # Initialize batch data provider
    data_provider = BatchPolarsDataProvider(device, config_dict)
    log_memory_usage("after data provider initialization")

    # Run pipeline with S3 upload
    datasets = data_provider.load_and_prepare_data_with_s3(
        save_datasets=save_datasets, output_dir=output_dir
    )
    log_memory_usage("after complete pipeline execution")
    force_garbage_collection()

    logger.info("=== Batch Data Preparation Pipeline Complete ===")

    # Return summary information
    train_dataset = datasets["train"]

    # Handle different dataset types for summary
    num_features = None
    sequence_length = None

    # Get sequences using the correct attribute name
    if hasattr(train_dataset, "X") and train_dataset.X is not None:
        sequences = train_dataset.X
        num_features = sequences.shape[2]
        sequence_length = sequences.shape[1]
    elif hasattr(train_dataset, "sequences") and train_dataset.sequences is not None:
        sequences = train_dataset.sequences
        num_features = sequences.shape[2]
        sequence_length = sequences.shape[1]
    else:
        # For sequence datasets, extract info from the dataset structure
        logger.info("Extracting summary info from sequence dataset...")
        if hasattr(train_dataset, "original_dataset"):
            # Undersampled sequence dataset
            original_dataset = train_dataset.original_dataset
            num_features = len(original_dataset.features)
            sequence_length = original_dataset.sequence_length
        elif hasattr(train_dataset, "features"):
            # Direct sequence dataset
            num_features = len(train_dataset.features)
            sequence_length = train_dataset.sequence_length
        else:
            logger.warning(
                "Could not determine num_features and sequence_length from sequence dataset"
            )
            num_features = 60  # Default fallback based on typical feature count
            sequence_length = 128  # Default fallback

    summary = {
        "datasets": datasets,
        "output_dir": output_dir if save_datasets else None,
        "device": str(device),
        "num_features": num_features,
        "sequence_length": sequence_length,
        "splits": {
            split_name: len(dataset)
            for split_name, dataset in datasets.items()
            if split_name != "s3_summary"
        },
        "s3_summary": datasets.get("s3_summary", {}),
    }

    log_memory_usage("at summary creation")
    return summary


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
    """Main function for batch data preparation pipeline."""
    import argparse

    parser = argparse.ArgumentParser(
        description="Batch Polars Data Preparation Pipeline with S3 Upload"
    )
    parser.add_argument(
        "--config",
        type=str,
        required=True,
        help="Configuration file (local path or s3://bucket/key)",
    )
    parser.add_argument(
        "--output-dir",
        type=str,
        default="/tmp/prepared_datasets",
        help="Local output directory",
    )
    parser.add_argument("--no-save", action="store_true", help="Do not save datasets")

    args = parser.parse_args()

    try:
        # Load configuration
        logger.info(f"Loading configuration from {args.config}...")
        config_dict = load_config(args.config)

        # Run pipeline
        summary = prepare_data_pipeline_with_s3(
            config_dict=config_dict,
            save_datasets=not args.no_save,
            output_dir=args.output_dir,
        )

        # Print summary
        logger.info("\n=== BATCH PIPELINE SUMMARY ===")
        logger.info(f"Device: {summary['device']}")
        logger.info(f"Sequence Length: {summary['sequence_length']}")
        logger.info(f"Number of Features: {summary['num_features']}")
        logger.info(f"Dataset Splits:")
        for split_name, count in summary["splits"].items():
            logger.info(f"  {split_name.capitalize()}: {count:,} samples")

        if summary["output_dir"]:
            logger.info(f"Local datasets saved to: {summary['output_dir']}")

        s3_summary = summary.get("s3_summary", {})
        if s3_summary:
            logger.info(f"S3 Bucket: {s3_summary.get('s3_bucket', 'N/A')}")
            logger.info(f"S3 Path: {s3_summary.get('s3_datasets_path', 'N/A')}")
            logger.info(f"Uploaded Files: {len(s3_summary.get('uploaded_files', []))}")

        logger.info("\n✓ Batch data preparation pipeline completed successfully!")

        # Save final summary for next pipeline stage
        final_summary_path = Path(args.output_dir) / "pipeline_summary.json"
        with open(final_summary_path, "w") as f:
            json.dump(summary, f, indent=2, default=str)
        logger.info(f"✓ Final summary saved to: {final_summary_path}")

    except Exception as e:
        logger.error(f"Batch pipeline failed: {e}")
        import traceback

        traceback.print_exc()
        return 1

    return 0


if __name__ == "__main__":
    exit(main())
