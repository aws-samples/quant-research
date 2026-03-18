#!/usr/bin/env python3

# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: MIT-0

"""
AWS Batch pipeline submission script for ML training pipeline.
This script submits both data preparation and model training jobs to AWS Batch.
"""

import boto3
import json
import argparse
import time
from datetime import datetime
from typing import Dict, List, Any, Optional
import logging

# Set up logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger("batch_pipeline_submitter")


class BatchPipelineSubmitter:
    """Submits and manages ML pipeline jobs on AWS Batch."""

    def __init__(self, region: str = "us-east-1"):
        self.batch_client = boto3.client("batch", region_name=region)
        self.s3_client = boto3.client("s3", region_name=region)
        self.region = region

    def submit_data_preparation_job(
        self,
        job_queue: str,
        job_definition: str,
        job_name: str,
        config_s3_path: str,
        instrument: str,
        start_date: str,
        end_date: str,
        s3_bucket: str,
        memory: int = 131072,
        vcpus: int = 16,
    ) -> Dict[str, Any]:
        """Submit data preparation job to AWS Batch."""

        job_name_full = f"{job_name}-data-prep-{instrument}-{start_date}-{end_date}"

        # Environment variables for the job
        environment = [
            {"name": "AWS_DEFAULT_REGION", "value": self.region},
            {"name": "S3_BUCKET_NAME", "value": s3_bucket},
            {"name": "INSTRUMENT", "value": instrument},
            {"name": "START_DATE", "value": start_date},
            {"name": "END_DATE", "value": end_date},
        ]

        # Command to run data preparation
        command = [
            "python3",
            "/app/src/batch_polars_dataprovider.py",
            "--config",
            config_s3_path,
            "--output-dir",
            "/tmp/prepared_datasets",
        ]

        # Resource requirements
        resource_requirements = [
            {"type": "MEMORY", "value": str(memory)},
            {"type": "VCPU", "value": str(vcpus)},
        ]

        response = self.batch_client.submit_job(
            jobName=job_name_full,
            jobQueue=job_queue,
            jobDefinition=job_definition,
            parameters={},
            containerOverrides={
                "command": command,
                "environment": environment,
                "resourceRequirements": resource_requirements,
            },
        )

        job_id = response["jobId"]
        logger.info(f"✓ Submitted data preparation job: {job_name_full} (ID: {job_id})")

        return {
            "job_id": job_id,
            "job_name": job_name_full,
            "type": "data_preparation",
            "instrument": instrument,
            "start_date": start_date,
            "end_date": end_date,
        }

    def submit_training_job(
        self,
        job_queue: str,
        job_definition: str,
        job_name: str,
        config_s3_path: str,
        instrument: str,
        start_date: str,
        end_date: str,
        s3_bucket: str,
        models: List[str],
        memory: int = 22528,
        vcpus: int = 8,
        depends_on: Optional[List[Dict[str, str]]] = None,
    ) -> Dict[str, Any]:
        """Submit model training job to AWS Batch."""

        job_name_full = f"{job_name}-ml-training-{instrument}-{start_date}-{end_date}"

        # Environment variables for the job
        environment = [
            {"name": "AWS_DEFAULT_REGION", "value": self.region},
            {"name": "S3_BUCKET_NAME", "value": s3_bucket},
            {"name": "INSTRUMENT", "value": instrument},
            {"name": "START_DATE", "value": start_date},
            {"name": "END_DATE", "value": end_date},
        ]

        # Command to run training - matches your actual usage
        command = ["python3", "/app/src/batch_train.py", "--config", config_s3_path]

        # Resource requirements
        resource_requirements = [
            {"type": "MEMORY", "value": str(memory)},
            {"type": "VCPU", "value": str(vcpus)},
        ]

        # Add dependency if specified
        job_kwargs = {
            "jobName": job_name_full,
            "jobQueue": job_queue,
            "jobDefinition": job_definition,
            "parameters": {},
            "containerOverrides": {
                "command": command,
                "environment": environment,
                "resourceRequirements": resource_requirements,
            },
        }

        if depends_on:
            job_kwargs["dependsOn"] = depends_on

        response = self.batch_client.submit_job(**job_kwargs)

        job_id = response["jobId"]
        logger.info(f"✓ Submitted training job: {job_name_full} (ID: {job_id})")

        return {
            "job_id": job_id,
            "job_name": job_name_full,
            "type": "model_training",
            "instrument": instrument,
            "start_date": start_date,
            "end_date": end_date,
            "models": models,
        }

    def wait_for_job(self, job_id: str, max_wait_time: int = 3600) -> str:
        """Wait for a job to complete and return its status."""
        logger.info(f"Waiting for job {job_id} to complete...")

        start_time = time.time()
        while time.time() - start_time < max_wait_time:
            response = self.batch_client.describe_jobs(jobs=[job_id])

            if not response["jobs"]:
                logger.error(f"Job {job_id} not found")
                return "NOT_FOUND"

            job = response["jobs"][0]
            status = job["jobStatus"]

            if status in ["SUCCEEDED", "FAILED"]:
                logger.info(f"Job {job_id} completed with status: {status}")
                return status

            logger.info(f"Job {job_id} status: {status}")
            time.sleep(30)  # Wait 30 seconds before checking again

        logger.warning(f"Job {job_id} did not complete within {max_wait_time} seconds")
        return "TIMEOUT"

    def upload_config_to_s3(
        self, config_dict: Dict[str, Any], s3_bucket: str, s3_key: str
    ) -> str:
        """Upload configuration file to S3."""

        # Replace environment variable placeholders
        config_str = json.dumps(config_dict, indent=2)
        config_str = config_str.replace("${S3_BUCKET_NAME}", s3_bucket)

        try:
            self.s3_client.put_object(
                Bucket=s3_bucket,
                Key=s3_key,
                Body=config_str,
                ContentType="application/json",
            )

            s3_path = f"s3://{s3_bucket}/{s3_key}"
            logger.info(f"✓ Uploaded config to S3: {s3_path}")
            return s3_path

        except Exception as e:
            logger.error(f"Failed to upload config to S3: {e}")
            raise

    def run_full_pipeline(
        self,
        data_prep_job_queue: str,
        training_job_queue: str,
        job_definition: str,
        job_name_prefix: str,
        config_dict: Dict[str, Any],
        s3_bucket: str,
        instrument: str,
        start_date: str,
        end_date: str,
        models: List[str],
        data_prep_memory: int = 131072,
        data_prep_vcpus: int = 16,
        training_memory: int = 22528,
        training_vcpus: int = 8,
        wait_for_completion: bool = True,
    ) -> Dict[str, Any]:
        """Run the complete ML pipeline: data preparation → model training."""

        logger.info("=== Starting AWS Batch ML Pipeline ===")

        # Step 1: Upload configuration to S3
        config_s3_key = (
            f"configs/batch_config_{instrument}_{start_date}_{end_date}.json"
        )
        config_s3_path = self.upload_config_to_s3(config_dict, s3_bucket, config_s3_key)

        # Step 2: Submit data preparation job
        logger.info("Step 1: Submitting data preparation job...")
        data_prep_job = self.submit_data_preparation_job(
            job_queue=data_prep_job_queue,
            job_definition=job_definition,
            job_name=job_name_prefix,
            config_s3_path=config_s3_path,
            instrument=instrument,
            start_date=start_date,
            end_date=end_date,
            s3_bucket=s3_bucket,
            memory=data_prep_memory,
            vcpus=data_prep_vcpus,
        )

        # Step 3: Wait for data preparation to complete (if requested)
        if wait_for_completion:
            data_prep_status = self.wait_for_job(data_prep_job["job_id"])
            if data_prep_status != "SUCCEEDED":
                raise RuntimeError(
                    f"Data preparation job failed with status: {data_prep_status}"
                )

        # Step 4: Submit training job with dependency on data preparation
        logger.info("Step 2: Submitting model training job...")
        training_job = self.submit_training_job(
            job_queue=training_job_queue,
            job_definition=job_definition,
            job_name=job_name_prefix,
            config_s3_path=config_s3_path,
            instrument=instrument,
            start_date=start_date,
            end_date=end_date,
            s3_bucket=s3_bucket,
            models=models,
            memory=training_memory,
            vcpus=training_vcpus,
            depends_on=(
                [{"jobId": data_prep_job["job_id"]}]
                if not wait_for_completion
                else None
            ),
        )

        # Step 5: Wait for training to complete (if requested)
        training_status = None
        if wait_for_completion:
            training_status = self.wait_for_job(training_job["job_id"])
            if training_status != "SUCCEEDED":
                logger.warning(f"Training job completed with status: {training_status}")

        logger.info("=== AWS Batch ML Pipeline Complete ===")

        # Return summary
        pipeline_summary = {
            "pipeline_id": f"{job_name_prefix}-{instrument}-{start_date}-{end_date}",
            "submitted_at": datetime.now().isoformat(),
            "config_s3_path": config_s3_path,
            "data_preparation_job": data_prep_job,
            "training_job": training_job,
            "data_prep_status": (
                data_prep_status if wait_for_completion else "SUBMITTED"
            ),
            "training_status": training_status if wait_for_completion else "SUBMITTED",
            "s3_bucket": s3_bucket,
            "s3_datasets_path": f"ml-pipeline/datasets/instrument={instrument}/start_date={start_date}/end_date={end_date}/",
            "s3_models_path": f"ml-pipeline/models/instrument={instrument}/start_date={start_date}/end_date={end_date}/",
        }

        return pipeline_summary


def main():
    """Main function to submit batch pipeline jobs."""
    parser = argparse.ArgumentParser(description="Submit ML Pipeline Jobs to AWS Batch")
    parser.add_argument("--config", type=str, required=True, help="Configuration file")
    parser.add_argument(
        "--data-prep-job-queue",
        type=str,
        required=True,
        help="AWS Batch job queue for data preparation",
    )
    parser.add_argument(
        "--training-job-queue",
        type=str,
        help="AWS Batch job queue for training (defaults to data prep queue)",
    )
    parser.add_argument(
        "--job-definition", type=str, required=True, help="AWS Batch job definition"
    )
    parser.add_argument(
        "--job-name", type=str, default="ml-pipeline", help="Job name prefix"
    )
    parser.add_argument("--s3-bucket", type=str, required=True, help="S3 bucket name")
    parser.add_argument("--instrument", type=str, required=True, help="Instrument ID")
    parser.add_argument(
        "--start-date", type=str, required=True, help="Start date (YYYY-MM-DD)"
    )
    parser.add_argument(
        "--end-date", type=str, required=True, help="End date (YYYY-MM-DD)"
    )
    parser.add_argument(
        "--models", nargs="+", default=["CNN_LSTM"], help="Models to train"
    )
    parser.add_argument(
        "--data-prep-memory",
        type=int,
        default=131072,
        help="Memory for data prep job (MB)",
    )
    parser.add_argument(
        "--data-prep-vcpus", type=int, default=16, help="vCPUs for data prep job"
    )
    parser.add_argument(
        "--training-memory",
        type=int,
        default=22528,
        help="Memory for training job (MB)",
    )
    parser.add_argument(
        "--training-vcpus", type=int, default=8, help="vCPUs for training job"
    )
    parser.add_argument("--region", type=str, default="us-east-1", help="AWS region")
    parser.add_argument(
        "--no-wait", action="store_true", help="Do not wait for jobs to complete"
    )

    # Support legacy --job-queue argument for backward compatibility
    parser.add_argument(
        "--job-queue",
        type=str,
        help="AWS Batch job queue (legacy, use --data-prep-job-queue)",
    )

    args = parser.parse_args()

    # Handle legacy --job-queue argument
    if args.job_queue and not args.data_prep_job_queue:
        args.data_prep_job_queue = args.job_queue

    # Default training queue to data prep queue if not specified
    if not args.training_job_queue:
        args.training_job_queue = args.data_prep_job_queue

    try:
        # Load configuration
        logger.info(f"Loading configuration from {args.config}...")
        with open(args.config, "r") as f:
            config_dict = json.load(f)

        # Override config with command line arguments
        config_dict["data"]["instrument_filter"] = args.instrument
        config_dict["data"]["start_date"] = args.start_date
        config_dict["data"]["end_date"] = args.end_date
        config_dict["s3"]["bucket_name"] = args.s3_bucket

        logger.info(f"✓ Loaded configuration from {args.config}")

        # Initialize submitter
        submitter = BatchPipelineSubmitter(region=args.region)

        # Run pipeline
        pipeline_summary = submitter.run_full_pipeline(
            data_prep_job_queue=args.data_prep_job_queue,
            training_job_queue=args.training_job_queue,
            job_definition=args.job_definition,
            job_name_prefix=args.job_name,
            config_dict=config_dict,
            s3_bucket=args.s3_bucket,
            instrument=args.instrument,
            start_date=args.start_date,
            end_date=args.end_date,
            models=args.models,
            data_prep_memory=args.data_prep_memory,
            data_prep_vcpus=args.data_prep_vcpus,
            training_memory=args.training_memory,
            training_vcpus=args.training_vcpus,
            wait_for_completion=not args.no_wait,
        )

        # Print summary
        logger.info(f"\n{'='*60}")
        logger.info("PIPELINE SUMMARY")
        logger.info(f"{'='*60}")
        logger.info(f"Pipeline ID: {pipeline_summary['pipeline_id']}")
        logger.info(
            f"Data Prep Job: {pipeline_summary['data_preparation_job']['job_name']}"
        )
        logger.info(f"  Job ID: {pipeline_summary['data_preparation_job']['job_id']}")
        logger.info(f"  Status: {pipeline_summary['data_prep_status']}")
        logger.info(f"Training Job: {pipeline_summary['training_job']['job_name']}")
        logger.info(f"  Job ID: {pipeline_summary['training_job']['job_id']}")
        logger.info(f"  Status: {pipeline_summary['training_status']}")
        logger.info(
            f"  Models: {', '.join(pipeline_summary['training_job']['models'])}"
        )
        logger.info(
            f"S3 Datasets: s3://{args.s3_bucket}/{pipeline_summary['s3_datasets_path']}"
        )
        logger.info(
            f"S3 Models: s3://{args.s3_bucket}/{pipeline_summary['s3_models_path']}"
        )
        logger.info(f"{'='*60}")

        # Save summary to file
        summary_filename = (
            f"pipeline_summary_{args.instrument}_{args.start_date}_{args.end_date}.json"
        )
        with open(summary_filename, "w") as f:
            json.dump(pipeline_summary, f, indent=2, default=str)

        logger.info(f"✓ Pipeline summary saved to: {summary_filename}")
        logger.info(f"✓ Pipeline submitted successfully!")

        # Instructions for monitoring
        if args.no_wait:
            logger.info(f"\nTo monitor jobs:")
            logger.info(
                f"  Data Prep: aws batch describe-jobs --jobs {pipeline_summary['data_preparation_job']['job_id']}"
            )
            logger.info(
                f"  Training: aws batch describe-jobs --jobs {pipeline_summary['training_job']['job_id']}"
            )
            logger.info(f"\nTo check S3 results:")
            logger.info(
                f"  Datasets: aws s3 ls s3://{args.s3_bucket}/{pipeline_summary['s3_datasets_path']}"
            )
            logger.info(
                f"  Models: aws s3 ls s3://{args.s3_bucket}/{pipeline_summary['s3_models_path']}"
            )

    except Exception as e:
        logger.error(f"❌ Pipeline submission failed: {e}")
        import traceback

        traceback.print_exc()
        return 1

    return 0


if __name__ == "__main__":
    exit(main())
