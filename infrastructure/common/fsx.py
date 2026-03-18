# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: MIT-0

from typing import Dict, Optional

from aws_cdk import Stack, aws_ec2 as ec2, aws_fsx as fsx, Environment, Tags
from constructs import Construct


class FSxStack(Stack):
    """Stack for creating an FSx for Lustre file system with optional data repository associations."""

    # Class constants
    FILE_SYSTEM_TYPE = "LUSTRE"
    FILE_SYSTEM_VERSION = "2.15"
    REQUIRED_CONFIG_FIELDS = {"data_repository_path", "file_system_path"}

    def __init__(
        self,
        scope: Construct,
        construct_id: str,
        env: Environment,
        namespace: str,
        subnet: ec2.ISubnet,
        security_group: ec2.ISecurityGroup,
        data_repository_associations: Optional[Dict] = None,
        per_unit_storage_throughput: int = 200,
        storage_capacity_gib: int = 1200,
        deployment_type: str = fsx.LustreDeploymentType.SCRATCH_2.value,
        **kwargs,
    ) -> None:
        super().__init__(scope, construct_id, env=env, **kwargs)

        # Validate inputs
        self._validate_inputs(storage_capacity_gib, per_unit_storage_throughput)

        # Store common properties
        self.namespace = namespace
        self.deployment_type = str(deployment_type)

        # Create resources
        self.lustre_fs = self._create_lustre_filesystem(
            subnet=subnet,
            security_group=security_group,
            per_unit_storage_throughput=per_unit_storage_throughput,
            storage_capacity_gib=storage_capacity_gib,
        )

        self.lustre_data_repository_associations = (
            self._create_data_repository_associations(data_repository_associations)
            if data_repository_associations
            else {}
        )

    @staticmethod
    def _validate_inputs(storage_capacity: int, throughput: int) -> None:
        """Validate input parameters."""
        if storage_capacity < 1200:
            raise ValueError("Storage capacity must be at least 1200 GiB")
        if throughput < 50:
            raise ValueError("Per unit storage throughput must be at least 50 MB/s")

    def _create_lustre_configuration(
        self, per_unit_storage_throughput: int
    ) -> fsx.CfnFileSystem.LustreConfigurationProperty:
        """Create Lustre configuration with proper throughput settings."""
        return fsx.CfnFileSystem.LustreConfigurationProperty(
            deployment_type=self.deployment_type,
            per_unit_storage_throughput=(
                per_unit_storage_throughput
                if self.deployment_type == fsx.LustreDeploymentType.PERSISTENT_2.value
                else None
            ),
        )

    def _create_lustre_filesystem(
        self,
        subnet: ec2.ISubnet,
        security_group: ec2.ISecurityGroup,
        per_unit_storage_throughput: int,
        storage_capacity_gib: int,
    ) -> fsx.CfnFileSystem:
        """Create the FSx Lustre file system."""
        filesystem = fsx.CfnFileSystem(
            self,
            "FSxLustreFileSystem",
            file_system_type=self.FILE_SYSTEM_TYPE,
            file_system_type_version=self.FILE_SYSTEM_VERSION,
            subnet_ids=[subnet.subnet_id],
            security_group_ids=[security_group.security_group_id],
            storage_capacity=storage_capacity_gib,
            lustre_configuration=self._create_lustre_configuration(
                per_unit_storage_throughput
            ),
        )

        # Add tags
        Tags.of(filesystem).add("Name", f"{self.namespace}-fsx")

        return filesystem

    def _create_data_repository_associations(self, configs: Dict) -> Dict:
        """Create data repository associations for the FSx file system."""
        return {
            name: self._create_single_repository_association(name, config)
            for name, config in configs.items()
        }

    def _create_single_repository_association(
        self, name: str, config: Dict
    ) -> fsx.CfnDataRepositoryAssociation:
        """Create a single data repository association."""
        missing_fields = self.REQUIRED_CONFIG_FIELDS - config.keys()
        if missing_fields:
            raise ValueError(f"Missing required fields: {missing_fields}")

        return fsx.CfnDataRepositoryAssociation(
            self,
            name,
            file_system_id=self.lustre_fs.ref,
            data_repository_path=config["data_repository_path"],
            file_system_path=config["file_system_path"],
            batch_import_meta_data_on_create=False,
        )
