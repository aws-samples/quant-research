# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: MIT-0

import aws_cdk as core
from aws_cdk.assertions import Match, Template

from infrastructure.common.network import NetworkStack


def test_network_created():
    app = core.App()
    env = core.Environment(account="012345678901", region="us-east-1")

    stack = NetworkStack(
        app,
        "infrastructure",
        env=env,
        namespace="test-quant-research",
        availability_zone="us-east-1",
        with_s3express=True,
    )
    template = Template.from_stack(stack)

    template.resource_count_is("AWS::EC2::VPC", 1)
    template.resource_count_is("AWS::EC2::VPCEndpoint", 15)
    template.resource_count_is("AWS::EC2::Subnet", 1)

    template.has_resource_properties(
        "AWS::EC2::SecurityGroup",
        {
            "SecurityGroupEgress": [{"IpProtocol": "-1", "CidrIp": "0.0.0.0/0"}],
            "SecurityGroupIngress": [
                {
                    "IpProtocol": "tcp",
                    "FromPort": 443,
                    "ToPort": 443,
                    "CidrIp": Match.any_value(),
                }
            ],
        },
    )
