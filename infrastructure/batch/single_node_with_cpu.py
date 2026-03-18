# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: MIT-0

from dataclasses import dataclass
from typing import List, Optional

from aws_cdk import (
    Size,
    Stack,
    RemovalPolicy,
    aws_batch as batch,
    aws_ec2 as ec2,
    aws_ecs as ecs,
    aws_fsx as fsx,
    aws_logs as logs,
    Environment,
)
from constructs import Construct
from math import floor

from .base_construct import (
    S3BucketArnConfig,
    BatchJobConstruct,
    BatchJobConstructConfig,
)


@dataclass
class BatchJobConfigForSingleNodeWithCPU:
    """Configuration for Single Node Batch Stack"""

    namespace: str
    vpc: ec2.IVpc
    security_group: ec2.SecurityGroup
    s3_bucket_config: S3BucketArnConfig
    lustre_fs: fsx.CfnFileSystem
    container_image_uri: str
    maxv_cpus: int
    minv_cpus: int = 0
    num_queues: int = 1
    container_memory: int = (
        32768  # Increased from 2GB to 32GB for memory-intensive ML workloads
    )
    container_cpu: int = 8  # Increased proportionally with memory
    container_command: Optional[List[str]] = None
    instance_classes: Optional[List[str]] = None
    allocation_strategy: str = "BEST_FIT_PROGRESSIVE"
    spot: bool = False


class BatchJobStackForSingleNodeWithCPU(Stack):

    def __init__(
        self,
        scope: Construct,
        construct_id: str,
        env: Environment,
        config: BatchJobConfigForSingleNodeWithCPU,
        **kwargs,
    ) -> None:
        super().__init__(scope, construct_id, env=env, **kwargs)

        self.job_queues = {}
        self.compute_environments = {}

        self.namespace = config.namespace
        self.lustre_fs = config.lustre_fs
        self.s3_bucket_config = config.s3_bucket_config

        # Building IAM permissions
        self.batch_job_construct = BatchJobConstruct(
            self,
            "BatchJobIAMPermissions",
            config=BatchJobConstructConfig(
                namespace=self.namespace,
                s3_bucket_config=self.s3_bucket_config,
            ),
        )

        # Building EC2 launch template
        self.user_data = self.build_user_data()
        self.launch_template = self.build_launch_template()

        # Building the compute environment
        self.build_compute_environment(config)
        self.job_definition = self.build_job_definition(config)

        # Added dependency for orchestrating infrastructure build
        self.job_definition.node.add_dependency(
            *list(self.job_queues.values()),
            *list(self.compute_environments.values()),
        )

        if self.lustre_fs is not None:
            self.job_definition.node.add_dependency(self.lustre_fs)

    def build_job_definition(
        self, config: BatchJobConfigForSingleNodeWithCPU
    ) -> batch.EcsJobDefinition:
        """
        Creates AWS Batch job definition
        """
        return batch.EcsJobDefinition(
            self,
            "BatchJobDefinition",
            propagate_tags=True,
            job_definition_name=f"{self.namespace}-single-node-with-cpu",
            container=batch.EcsEc2ContainerDefinition(
                self,
                "BatchJobContainerDefinition",
                image=ecs.ContainerImage.from_registry(config.container_image_uri),
                command=(
                    config.container_command
                    if config.container_command
                    else [
                        "python3",
                        "main.py",
                    ]
                ),
                memory=Size.mebibytes(config.container_memory),
                cpu=config.container_cpu,
                job_role=self.batch_job_construct.job_role,
                execution_role=self.batch_job_construct.task_execution_role,
                logging=ecs.LogDriver.aws_logs(
                    stream_prefix=config.namespace,
                    log_group=logs.LogGroup(
                        self,
                        "BatchJobLogGroup",
                        log_group_name=f"/{self.namespace}/batch-job/single-node-with-cpu",
                        retention=logs.RetentionDays.ONE_WEEK,
                        removal_policy=RemovalPolicy.DESTROY,
                    ),
                    mode=ecs.AwsLogDriverMode.NON_BLOCKING,
                    max_buffer_size=Size.mebibytes(128),
                ),
                volumes=(
                    [
                        batch.HostVolume(
                            name="fsx",
                            host_path=self.batch_job_construct.MOUNT_PATH,
                            container_path=self.batch_job_construct.MOUNT_PATH,
                            readonly=False,
                        )
                    ]
                    if config.lustre_fs is not None
                    else []
                ),
            ),
        )

    def build_compute_environment(self, config: BatchJobConfigForSingleNodeWithCPU):
        """
        Creates AWS Batch compute environment.
        """
        for i in range(config.num_queues):
            self.compute_environments[i] = batch.ManagedEc2EcsComputeEnvironment(
                self,
                f"BatchJobComputeEnvironment_{i:02}",
                compute_environment_name=f"{self.namespace}-single-node-with-cpu-{i:02}",
                instance_role=self.batch_job_construct.instance_role,
                launch_template=self.launch_template,
                instance_classes=(
                    [ec2.InstanceClass(c) for c in config.instance_classes]
                    if config.instance_classes
                    else [
                        ec2.InstanceClass.R5,
                        ec2.InstanceClass.R6A,
                        ec2.InstanceClass.R6I,
                        ec2.InstanceClass.R5A,
                        ec2.InstanceClass.R7I,
                        ec2.InstanceClass.R5N,
                        ec2.InstanceClass.R5D,
                        ec2.InstanceClass.R5B,
                    ]
                ),
                vpc_subnets=ec2.SubnetSelection(
                    subnet_type=ec2.SubnetType.PRIVATE_ISOLATED
                ),
                vpc=config.vpc,
                allocation_strategy=batch.AllocationStrategy(
                    config.allocation_strategy
                ),
                spot=config.spot,
                update_to_latest_image_version=True,
                use_optimal_instance_classes=False,
                security_groups=[config.security_group],
                minv_cpus=config.minv_cpus,
                maxv_cpus=floor(config.maxv_cpus / config.num_queues),
            )

            self.job_queues[i] = batch.JobQueue(
                self,
                f"BatchJobQueue_{i:02}",
                job_queue_name=f"{self.namespace}-single-node-with-cpu-{i:02}",
                compute_environments=[
                    batch.OrderedComputeEnvironment(
                        compute_environment=self.compute_environments[i], order=1
                    )
                ],
            )

    def build_launch_template(self) -> ec2.LaunchTemplate:
        """
        Creates an EC2 launch template for AWS Batch with increased root volume size.
        """
        return ec2.LaunchTemplate(
            self,
            "BatchJobLaunchTemplate",
            launch_template_name=f"{self.namespace}-single-node-with-cpu",
            machine_image=ec2.MachineImage.from_ssm_parameter(
                "/aws/service/ecs/optimized-ami/amazon-linux-2023/recommended/image_id"
            ),
            detailed_monitoring=True,
            user_data=self.user_data,
            # Increase root volume size for ML storage
            block_devices=[
                ec2.BlockDevice(
                    device_name="/dev/xvda",  # Root device
                    volume=ec2.BlockDeviceVolume.ebs(
                        volume_size=100,  # Increase root volume to 100GB
                        volume_type=ec2.EbsDeviceVolumeType.GP3,
                        encrypted=True,
                        delete_on_termination=True,
                        iops=3000,  # High performance IOPS
                        throughput=125,  # MiB/s throughput for GP3
                    ),
                )
            ],
        )

    def build_user_data(self):
        """
        Creates a user data script for compute environment EC2 instances.
        """
        user_data = ec2.MultipartUserData()
        user_data.add_user_data_part(ec2.UserData.for_linux(), make_default=True)

        if self.lustre_fs is not None:
            # Add lustre filesystem condition
            user_data.add_commands(
                f"fsx_directory={self.batch_job_construct.MOUNT_PATH}",
                "dnf install -y lustre-client",
                "mkdir -p ${fsx_directory}",
                f"mount -t lustre -o defaults,sync,relatime,flock,_netdev,x-systemd.automount,x-systemd.requires=network.service {self.lustre_fs.attr_dns_name}@tcp:/{self.lustre_fs.attr_lustre_mount_name} ${{fsx_directory}}",
                "if [ $? -ne 0 ]; then shutdown -P -h now; fi",
                f"cat <<EOF > /usr/lib/systemd/system/fsx.service\n[Unit]\nDescription=Unmount FSX\nBefore=shutdown.target reboot.target halt.target\nRequires=network-online.target network.target\n\n[Service]\nKillMode=none\nExecStart=/bin/true\nExecStop=umount {self.batch_job_construct.MOUNT_PATH}\nRemainAfterExit=yes\nType=oneshot\n\n[Install]\nWantedBy=multi-user.target\nEOF",
                "systemctl enable fsx.service",
                "systemctl start fsx.service",
            )
        return user_data
