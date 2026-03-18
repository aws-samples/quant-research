# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: MIT-0

from unittest.mock import Mock

import aws_cdk as core
from aws_cdk import aws_ec2 as ec2, aws_fsx as fsx, aws_s3 as s3
from aws_cdk.assertions import Template

from infrastructure.common.fsx import FSxStack


def test_fsx_created():
    app = core.App()
    env = core.Environment(account="012345678901", region="us-east-1")

    # Create mock subnet
    mock_subnet = Mock(spec=ec2.ISubnet)
    mock_subnet.subnet_id = "subnet-12345"
    mock_subnet.availability_zone = "us-east-1a"

    # Create mock security group
    mock_security_group = Mock(spec=ec2.ISecurityGroup)
    mock_security_group.security_group_id = "sg-12345"

    # Create basic mock S3 bucket
    mock_bucket = Mock(spec=s3.Bucket)
    mock_bucket.bucket_name = "bucket-12345"
    mock_bucket.bucket_arn = "arn:aws:s3:::bucket-12345"
    mock_bucket.bucket_domain_name = "bucket-12345.s3.us-east-1.amazonaws.com"

    # Create the standard_bucket attribute
    mock_bucket.standard_bucket = Mock(spec=s3.IBucket)
    mock_bucket.standard_bucket.bucket_name = "bucket-12345"
    mock_bucket.standard_bucket.bucket_arn = "arn:aws:s3:::bucket-12345"
    mock_bucket.standard_bucket.s3_url_for_object = Mock(
        return_value="s3://bucket-12345"
    )

    stack = FSxStack(
        app,
        "infrastructure",
        env=env,
        namespace="test-quant-research",
        subnet=mock_subnet,
        security_group=mock_security_group,
        data_repository_associations={
            "S3Bucket": {
                "data_repository_path": mock_bucket.standard_bucket.s3_url_for_object(),
                "file_system_path": "/scratch",
                "auto_export_on": ["NEW", "CHANGED", "DELETED"],
                "auto_import_on": ["NEW", "CHANGED", "DELETED"],
            }
        },
        per_unit_storage_throughput=200,
        storage_capacity_gib=1200,
        deployment_type=fsx.LustreDeploymentType.SCRATCH_2.value,
    )

    template = Template.from_stack(stack)

    template.resource_count_is("AWS::FSx::FileSystem", 1)
    template.resource_count_is("AWS::FSx::DataRepositoryAssociation", 1)

    template.has_resource_properties(
        "AWS::FSx::FileSystem",
        {
            "FileSystemType": "LUSTRE",
            "FileSystemTypeVersion": "2.15",
            "LustreConfiguration": {"DeploymentType": "SCRATCH_2"},
            "StorageCapacity": 1200,
        },
    )
