# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: MIT-0

import aws_cdk as core
from aws_cdk.assertions import Template

from infrastructure.common.s3 import S3Stack


def test_s3_created():
    app = core.App()
    env = core.Environment(account="012345678901", region="us-east-1")

    stack = S3Stack(
        app,
        "infrastructure",
        env=env,
        namespace="test-quant-research",
        availability_zone_id="use1-az4",
        s3_object_expiration_in_days=30,
        with_s3express=True,
    )
    template = Template.from_stack(stack)

    template.resource_count_is("AWS::S3::Bucket", 1)
    template.resource_count_is("AWS::S3Express::DirectoryBucket", 1)

    template.has_resource_properties(
        "AWS::S3::Bucket",
        {
            "BucketEncryption": {
                "ServerSideEncryptionConfiguration": [
                    {
                        "ServerSideEncryptionByDefault": {"SSEAlgorithm": "aws:kms"},
                        "BucketKeyEnabled": True,
                    }
                ]
            },
            "PublicAccessBlockConfiguration": {
                "BlockPublicAcls": True,
                "BlockPublicPolicy": True,
                "IgnorePublicAcls": True,
                "RestrictPublicBuckets": True,
            },
        },
    )
