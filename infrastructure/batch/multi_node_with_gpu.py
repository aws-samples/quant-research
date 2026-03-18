# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: MIT-0

from dataclasses import dataclass
from typing import List, Optional

from aws_cdk import (
    Stack,
    aws_batch as batch,
    aws_ec2 as ec2,
    aws_ecs as ecs,
    aws_fsx as fsx,
    aws_logs as logs,
    Environment,
    Size,
    RemovalPolicy,
)
from aws_cdk.aws_batch import ManagedEc2EcsComputeEnvironment
from constructs import Construct

from .base_construct import (
    S3BucketArnConfig,
    BatchJobConstruct,
    BatchJobConstructConfig,
)


@dataclass
class BatchJobConfigForNodeWithGPU:
    """Configuration for the main/ worker compute node"""

    container_image_uri: str
    container_memory: int = 2048
    container_cpu: int = 2
    container_gpu: int = 0
    container_command: Optional[List[str]] = None
    start_node_index: int = 0
    end_node_index: int = 0


@dataclass
class BatchJobConfigForMultiNodeWithGPU:
    """Configuration for the Multi Node Batch Job Stack"""

    namespace: str
    vpc: ec2.IVpc
    security_group: ec2.SecurityGroup
    s3_bucket_config: S3BucketArnConfig
    lustre_fs: fsx.CfnFileSystem
    main_config: BatchJobConfigForNodeWithGPU
    worker_config: BatchJobConfigForNodeWithGPU
    maxv_cpus: int
    minv_cpus: int = 0
    instance_classes: Optional[List[str]] = None
    allocation_strategy: str = "BEST_FIT_PROGRESSIVE"
    spot: bool = False


class BatchJobStackForMultiNodeWithGPU(Stack):

    def __init__(
        self,
        scope: Construct,
        construct_id: str,
        env: Environment,
        config: BatchJobConfigForMultiNodeWithGPU,
        **kwargs,
    ) -> None:
        super().__init__(scope, construct_id, env=env, **kwargs)

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
        self.efa_security_group = ec2.SecurityGroup(
            self,
            "BatchJobEFASecurityGroup",
            vpc=config.vpc,
            allow_all_outbound=True,
            disable_inline_rules=True,
        )
        self.efa_security_group.add_ingress_rule(
            ec2.Peer.security_group_id(self.efa_security_group.security_group_id),
            ec2.Port.all_traffic(),
            description=f"Allow EFA ingress for {self.namespace}",
        )

        # Building the compute environment
        self.compute_environment = self.build_compute_environment(config)
        self.job_queue = self.build_job_queue()
        self.job_definition = self.build_job_definition(config)

        if self.lustre_fs is not None:
            self.launch_template.node.add_dependency(self.lustre_fs)
            self.compute_environment.node.add_dependency(self.lustre_fs)

    def build_job_definition(self, config: BatchJobConfigForMultiNodeWithGPU):
        """
        Creates AWS Batch job definition
        """
        main_container_config = config.main_config
        worker_container_config = config.worker_config

        log_group = logs.LogGroup(
            self,
            "BatchJobLogGroup",
            log_group_name=f"/{self.namespace}/batch-job/multi-node-with-gpu",
            retention=logs.RetentionDays.ONE_WEEK,
            removal_policy=RemovalPolicy.DESTROY,
        )

        multi_node_job_definition = batch.MultiNodeJobDefinition(
            self,
            "MultiNodeJobDefinition",
            propagate_tags=True,
            job_definition_name=f"{self.namespace}-multi-node-with-gpu",
            containers=[
                batch.MultiNodeContainer(
                    container=batch.EcsEc2ContainerDefinition(
                        self,
                        "mainContainer",
                        image=ecs.ContainerImage.from_registry(
                            main_container_config.container_image_uri
                        ),
                        command=(
                            main_container_config.container_command
                            if main_container_config.container_command
                            else [
                                "python3",
                                "main.py",
                            ]
                        ),
                        memory=Size.mebibytes(main_container_config.container_memory),
                        cpu=main_container_config.container_cpu,
                        gpu=main_container_config.container_gpu,
                        job_role=self.batch_job_construct.job_role,
                        execution_role=self.batch_job_construct.task_execution_role,
                        logging=ecs.LogDriver.aws_logs(
                            stream_prefix=config.namespace,
                            log_group=log_group,
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
                            if self.lustre_fs is not None
                            else []
                        ),
                    ),
                    start_node=main_container_config.start_node_index,
                    end_node=main_container_config.end_node_index,
                )
            ],
        )

        linux_parameters = batch.LinuxParameters(self, "LinuxParameters")
        linux_parameters.add_devices(
            batch.Device(
                host_path="/dev/infiniband/uverbs0",
                container_path="/dev/infiniband/uverbs0",
                permissions=[
                    batch.DevicePermission.READ,
                    batch.DevicePermission.WRITE,
                    batch.DevicePermission.MKNOD,
                ],
            )
        )

        multi_node_job_definition.add_container(
            container=batch.EcsEc2ContainerDefinition(
                self,
                "workerContainer",
                image=ecs.ContainerImage.from_registry(
                    worker_container_config.container_image_uri
                ),
                command=(
                    worker_container_config.container_command
                    if worker_container_config.container_command
                    else [
                        "python3",
                        "main.py",
                    ]
                ),
                memory=Size.mebibytes(worker_container_config.container_memory),
                cpu=worker_container_config.container_cpu,
                gpu=worker_container_config.container_gpu,
                job_role=self.batch_job_construct.job_role,
                execution_role=self.batch_job_construct.task_execution_role,
                logging=ecs.LogDriver.aws_logs(
                    stream_prefix=config.namespace,
                    log_group=log_group,
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
                    if self.lustre_fs is not None
                    else []
                ),
                linux_parameters=linux_parameters,
            ),
            start_node=worker_container_config.start_node_index,
            end_node=worker_container_config.end_node_index,
        )

        return multi_node_job_definition

    def build_compute_environment(
        self, config: BatchJobConfigForMultiNodeWithGPU
    ) -> ManagedEc2EcsComputeEnvironment:
        """
        Creates AWS Batch compute environment.
        """
        return batch.ManagedEc2EcsComputeEnvironment(
            self,
            "BatchJobComputeEnvironment",
            compute_environment_name=f"{self.namespace}-multi-node-with-gpu",
            instance_role=self.batch_job_construct.instance_role,
            spot=config.spot,
            maxv_cpus=config.maxv_cpus,
            minv_cpus=config.minv_cpus,
            launch_template=self.launch_template,
            placement_group=ec2.PlacementGroup(
                self,
                "BatchJobComputeEnvironmentPG",
                strategy=ec2.PlacementGroupStrategy.CLUSTER,
            ),
            instance_classes=(
                [ec2.InstanceClass(c) for c in config.instance_classes]
                if config.instance_classes
                else [ec2.InstanceClass.G5]
            ),
            vpc_subnets=ec2.SubnetSelection(
                subnet_type=ec2.SubnetType.PRIVATE_ISOLATED
            ),
            vpc=config.vpc,
            allocation_strategy=batch.AllocationStrategy(config.allocation_strategy),
            update_to_latest_image_version=True,
            use_optimal_instance_classes=False,
            security_groups=[self.efa_security_group, config.security_group],
        )

    def build_job_queue(self) -> batch.JobQueue:
        return batch.JobQueue(
            self,
            "BatchJobQueue",
            job_queue_name=f"{self.namespace}-multi-node-with-gpu",
            compute_environments=[
                batch.OrderedComputeEnvironment(
                    compute_environment=self.compute_environment, order=1
                )
            ],
        )

    def build_launch_template(self) -> ec2.LaunchTemplate:
        """
        Creates an EC2 launch template for AWS Batch with increased root volume size.
        """
        return ec2.LaunchTemplate(
            self,
            "BatchLaunchTemplate",
            launch_template_name=f"{self.namespace}-multi-node-with-gpu",
            machine_image=ec2.MachineImage.from_ssm_parameter(
                "/aws/service/ecs/optimized-ami/amazon-linux-2/gpu/recommended/image_id"
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
                'mkdir -p /etc/ecs && echo "ECS_ENABLE_GPU_SUPPORT=true" >> /etc/ecs/ecs.config',
                "systemctl restart ecs",
            )
        return user_data
