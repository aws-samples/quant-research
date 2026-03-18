# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: MIT-0

from typing import List, Dict

from aws_cdk import Stack, aws_ec2 as ec2, Environment
from constructs import Construct


class NetworkStack(Stack):
    """
    Stack for creating a VPC with various VPC endpoints and security groups.
    """

    # Define constants for VPC configuration
    DEFAULT_CIDR = "172.0.0.0/16"
    DEFAULT_SUBNET_MASK = 16

    # Define required interface endpoints
    REQUIRED_ENDPOINTS = {
        "ECR": ec2.InterfaceVpcEndpointAwsService.ECR,
        "ECRDocker": ec2.InterfaceVpcEndpointAwsService.ECR_DOCKER,
        "ECS": ec2.InterfaceVpcEndpointAwsService.ECS,
        "ECSAgent": ec2.InterfaceVpcEndpointAwsService.ECS_AGENT,
        "ECSTelemetry": ec2.InterfaceVpcEndpointAwsService.ECS_TELEMETRY,
        "FSxLustre": ec2.InterfaceVpcEndpointAwsService.FSX,
        "Logs": ec2.InterfaceVpcEndpointAwsService.CLOUDWATCH_LOGS,
        "SSM": ec2.InterfaceVpcEndpointAwsService.SSM,
        "SSMMessages": ec2.InterfaceVpcEndpointAwsService.SSM_MESSAGES,
        "EC2Messages": ec2.InterfaceVpcEndpointAwsService.EC2_MESSAGES,
        "EC2": ec2.InterfaceVpcEndpointAwsService.EC2,
        "KMS": ec2.InterfaceVpcEndpointAwsService.KMS,
        "Batch": ec2.InterfaceVpcEndpointAwsService.BATCH,
        "Glue": ec2.InterfaceVpcEndpointAwsService.GLUE,
    }

    def __init__(
        self,
        scope: Construct,
        construct_id: str,
        env: Environment,
        namespace: str,
        availability_zone: str,
        with_s3express: bool = True,
        **kwargs,
    ) -> None:
        """
        Initialize NetworkStack.

        Args:
            scope: CDK Construct scope
            construct_id: Unique identifier for the stack
            env: Deployment environment for the stack
            namespace: Namespace for resource naming
            availability_zone: AZ where resources will be deployed
            with_s3express: Whether to include S3 Express endpoint
        """
        super().__init__(scope, construct_id, env=env, **kwargs)

        # Create VPC and related resources
        self.vpc = self._create_vpc(namespace, availability_zone, with_s3express)
        self.security_group = self._create_security_group(namespace)
        self.interface_endpoints = self._create_interface_endpoints()

    def _create_gateway_endpoints(self, with_s3express: bool) -> Dict:
        """Create gateway endpoint configurations."""
        endpoints = {
            "S3": ec2.GatewayVpcEndpointOptions(
                service=ec2.GatewayVpcEndpointAwsService.S3
            ),
        }

        if with_s3express:
            endpoints["S3Express"] = ec2.GatewayVpcEndpointOptions(
                service=ec2.GatewayVpcEndpointAwsService.S3_EXPRESS
            )

        return endpoints

    def _create_vpc(
        self, namespace: str, availability_zone: str, with_s3express: bool
    ) -> ec2.Vpc:
        """Create VPC with specified configuration."""
        return ec2.Vpc(
            self,
            "VPC",
            vpc_name=f"{namespace}-vpc",
            ip_addresses=ec2.IpAddresses.cidr(self.DEFAULT_CIDR),
            availability_zones=[availability_zone],
            nat_gateways=0,
            create_internet_gateway=False,
            enable_dns_hostnames=True,
            enable_dns_support=True,
            subnet_configuration=[
                ec2.SubnetConfiguration(
                    name="Private",
                    subnet_type=ec2.SubnetType.PRIVATE_ISOLATED,
                    cidr_mask=self.DEFAULT_SUBNET_MASK,
                )
            ],
            gateway_endpoints=self._create_gateway_endpoints(with_s3express),
        )

    def _create_security_group(self, namespace: str) -> ec2.SecurityGroup:
        """Create security group with internal traffic rules."""
        security_group = ec2.SecurityGroup(
            self,
            "SecurityGroup",
            security_group_name=f"{namespace}-sg",
            vpc=self.vpc,
            allow_all_outbound=True,
            description="Security group for internal traffic",
        )

        security_group.add_ingress_rule(
            peer=security_group,
            connection=ec2.Port.all_tcp(),
            description="Allow internal TCP traffic",
        )

        return security_group

    def _create_interface_endpoints(self) -> List[ec2.InterfaceVpcEndpoint]:
        """Create all required interface endpoints."""
        endpoints = []

        for name, service in self.REQUIRED_ENDPOINTS.items():
            endpoint = ec2.InterfaceVpcEndpoint(
                self,
                f"{name}InterfaceEndpoint",
                service=service,
                vpc=self.vpc,
                security_groups=[self.security_group],
            )
            endpoints.append(endpoint)

        return endpoints

    def add_interface_endpoint(
        self, name: str, service: ec2.InterfaceVpcEndpointAwsService
    ) -> ec2.InterfaceVpcEndpoint:
        """
        Add a new interface endpoint to the VPC.

        Args:
            name: Name of the endpoint
            service: AWS service for the endpoint
        """
        endpoint = ec2.InterfaceVpcEndpoint(
            self,
            f"{name}InterfaceEndpoint",
            service=service,
            vpc=self.vpc,
            security_groups=[self.security_group],
        )
        self.interface_endpoints.append(endpoint)
        return endpoint
