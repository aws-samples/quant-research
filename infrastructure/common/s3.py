# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: MIT-0

from typing import List, Optional

from aws_cdk import (
    RemovalPolicy,
    Stack,
    Duration,
    aws_s3 as s3,
    aws_s3express as s3x,
    Tags,
    Environment,
)
from constructs import Construct


class S3Stack(Stack):
    """Stack for creating S3 standard and express buckets with lifecycle rules."""

    def __init__(
        self,
        scope: Construct,
        construct_id: str,
        env: Environment,
        namespace: str,
        availability_zone_id: str,
        s3_object_expiration_in_days: int = 1,
        with_s3express: bool = True,
        **kwargs,
    ) -> None:
        """
        Initialize S3Stack.

        Args:
            scope: CDK Construct scope
            construct_id: Unique identifier for the stack
            env: Deployment environment for the stack
            namespace: Namespace for resource naming
            availability_zone_id: AZ ID for S3 Express bucket
            s3_object_expiration_in_days: Days after which objects are deleted
            with_s3express: Whether to create an S3 Express bucket
        """
        super().__init__(scope, construct_id, env=env, **kwargs)

        self.namespace = namespace.lower()
        self.availability_zone_id = availability_zone_id
        self.s3_object_expiration_in_days = s3_object_expiration_in_days

        # Create buckets
        self.standard_bucket = self._create_standard_bucket()
        self.express_bucket = self._create_express_bucket() if with_s3express else None

    def _create_lifecycle_rules(self) -> List[s3.LifecycleRule]:
        """Create lifecycle rules for S3 bucket."""
        return [
            s3.LifecycleRule(
                abort_incomplete_multipart_upload_after=Duration.days(
                    self.s3_object_expiration_in_days
                ),
                enabled=True,
                expiration=Duration.days(self.s3_object_expiration_in_days),
                id="auto-delete-rule",
            )
        ]

    def _create_standard_bucket(self) -> s3.Bucket:
        """Create standard S3 bucket with lifecycle rules."""
        bucket = s3.Bucket(
            self,
            "S3StandardBucket",
            bucket_name=self._build_bucket_name("standard"),
            auto_delete_objects=True,
            removal_policy=RemovalPolicy.DESTROY,
            lifecycle_rules=self._create_lifecycle_rules(),
            enforce_ssl=True,  # Enforce SSL for security
            encryption=s3.BucketEncryption.KMS_MANAGED,  # Enable encryption
            bucket_key_enabled=True,
            versioned=True,  # Enable versioning
            block_public_access=s3.BlockPublicAccess(
                block_public_acls=True,
                block_public_policy=True,
                ignore_public_acls=True,
                restrict_public_buckets=True,
            ),  # Block public access
        )

        # Add tags for better resource management
        Tags.of(bucket).add("Environment", self.namespace)
        Tags.of(bucket).add("ManagedBy", "CDK")

        return bucket

    def _create_express_bucket(self) -> s3x.CfnDirectoryBucket:
        """Create S3 Express directory bucket."""

        bucket = s3x.CfnDirectoryBucket(
            self,
            "S3ExpressDirectoryBucket",
            bucket_name=self._build_bucket_name("express"),
            data_redundancy="SingleAvailabilityZone",
            location_name=self.availability_zone_id,
        )

        # Add tags for better resource management
        Tags.of(bucket).add("Environment", self.namespace)
        Tags.of(bucket).add("ManagedBy", "CDK")

        return bucket

    def grant_read_write(self, principal: any) -> None:
        """
        Grant read/write permissions to both buckets.

        Args:
            principal: IAM principal to grant permissions to
        """
        self.standard_bucket.grant_read_write(principal)
        if self.express_bucket:
            # Note: S3 Express buckets might have different permission mechanisms
            # Add appropriate permission grants here
            pass

    def add_lifecycle_rule(
        self, expiration_days: int, prefix: Optional[str] = None
    ) -> None:
        """
        Add a new lifecycle rule to the standard bucket.

        Args:
            expiration_days: Days after which objects are deleted
            prefix: Optional prefix for the rule
        """
        self.standard_bucket.add_lifecycle_rule(
            expiration=Duration.days(expiration_days),
            prefix=prefix,
            enabled=True,
        )

    def _build_bucket_name(self, bucket_type: str) -> str:
        """
        Build and validate S3 bucket name.

        Args:
            bucket_type (str): Type of bucket ('standard' or 'express')

        Returns:
            str: Valid S3 bucket name with truncated namespace if needed

        Raises:
            ValueError: If invalid bucket type or resulting name would be too short
        """
        if not isinstance(bucket_type, str):
            raise ValueError("Bucket type must be a string")

        bucket_type = bucket_type.lower()

        # Define the static parts of the bucket name
        # Refer directory bucket naming rules - https://docs.aws.amazon.com/AmazonS3/latest/userguide/directory-bucket-naming-rules.html
        suffixes = {
            "express": f"-express-bucket--{self.availability_zone_id}--x-s3",
            "standard": f"-standard-bucket-{self.region}",
        }

        if bucket_type not in suffixes:
            raise ValueError(
                f"Invalid bucket type. Must be one of: {', '.join(suffixes.keys())}"
            )

        suffix = suffixes[bucket_type]
        max_namespace_length = 63 - len(suffix)

        # Truncate namespace if needed
        namespace = self.namespace[:max_namespace_length]

        bucket_name = f"{namespace}{suffix}".lower()

        # Final validation
        if len(bucket_name) < 3:
            raise ValueError(
                "Resulting bucket name would be too short even with truncation"
            )

        return bucket_name
