import json

from aws_cdk import (
    Stack,
    Aws,
    ArnFormat
)

from aws_cdk.aws_iam import (
    ManagedPolicy,
    PolicyStatement,
)

from aws_cdk.aws_ec2 import (
    InstanceType
)
from aws_cdk.aws_eks import (
    TaintEffect,
    CapacityType,
    NodegroupAmiType
)

from constructs import Construct

from aws_analytics_reference_architecture import (
        EmrEksCluster,
        NotebookPlatform,
        StudioAuthMode,
        SSOIdentityType,
        NotebookUserOptions, 
        NotebookManagedEndpointOptions,
        EmrEksNodegroupOptions
)


class AdxStack(Stack):

    def __init__(self, scope: Construct, construct_id: str, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

        cluster_name = "clusteremrpython"
        emr_eks = EmrEksCluster.get_or_create(self,
        eks_admin_role_arn="arn:aws:iam::614393260192:role/fdp",
        default_node_groups=False,
        eks_cluster_name=cluster_name
        )

        emr_eks.add_emr_eks_nodegroup('NotebookDriverCustom2', 
            nodegroup_name='notebook_driver_custom_endpoint',
            ami_type=NodegroupAmiType.AL2_X86_64,
            instance_types=[InstanceType('m5.large'), InstanceType('c5.large')],
            min_size=1,
            max_size=10,
            labels={'role': 'notebook','spark-role': 'driver','node-lifecycle': 'on-demand'},
            taints=[{'key': 'role', 'value': 'notebook', 'effect': TaintEffect.NO_SCHEDULE}]
        );

        emr_eks.add_emr_eks_nodegroup('NotebookExecutorCustom2', 
            nodegroup_name='notebook_executor_custom_endpoint',
            ami_type=NodegroupAmiType.AL2_X86_64,
            instance_types=[InstanceType('m5.2xlarge'), InstanceType('c5.2xlarge')],
            min_size=0,
            max_size=100,
            capacity_type=CapacityType.SPOT,
            labels={'role': 'notebook','spark-role': 'executor','node-lifecycle': 'spot'},
            taints=[{'key': 'role', 'value': 'notebook', 'effect': TaintEffect.NO_SCHEDULE}]
        );
        

        notebook_platform = NotebookPlatform(self, "platform-notebook",
            emr_eks=emr_eks,
            eks_namespace="emr",
            studio_name="platformemrpython",
            studio_auth_mode=StudioAuthMode.SSO
        )

        # If the S3 bucket is encrypted, add policy to the key for the role
        policy1 = ManagedPolicy(self, "MyPolicy1",
            statements=[
                PolicyStatement(
                    resources=["arn:aws:s3:::maystreet"] ,
                    actions=["s3:*"]
                ),
                PolicyStatement(
                    resources=[
                        Stack.format_arn(self,
                            account=Aws.ACCOUNT_ID,
                            region=Aws.REGION,
                            service="logs",
                            resource="*",
                            arn_format=ArnFormat.NO_RESOURCE_NAME
                        )
                    ],
                    actions=["logs:*"]
                )
            ]
        )

      
        notebook_platform.add_user( 
            [NotebookUserOptions(
                identity_name="maystreet", # make sure user already setup in AWS SSO 
                identity_type= SSOIdentityType.USER.value,
                notebook_managed_endpoints = [NotebookManagedEndpointOptions(
                    emr_on_eks_version = "emr-6.7.0-latest",
                    managed_endpoint_name = "ManagedEndpoint1",
                    execution_policy = policy1,
                    configuration_overrides =
                    {
                        "applicationConfiguration": [
                            {
                                "classification": "spark-defaults",
                                "properties": {
                                    "spark.executor.memory": "14G",
                                    "spark.driver.memory": "2G",
                                    "spark.kubernetes.executor.request.cores": "3.5",
                                    "spark.driver.cores": "1",
                                    "spark.sql.catalogImplementation": "hive",
                                    "spark.executor.cores": "4",
                                    "spark.dynamicAllocation.maxExecutors": "50",
                                    "spark.dynamicAllocation.shuffleTracking.enabled": "true",
                                    "spark.dynamicAllocation.shuffleTracking.timeout": "300s",
                                    "spark.kubernetes.driver.request.cores": "0.5",
                                    "spark.kubernetes.allocation.batch.size": "2",
                                    "spark.hadoop.hive.metastore.client.factory.class": "com.amazonaws.glue.catalog.metastore.AWSGlueDataCatalogHiveClientFactory",
                                    "spark.dynamicAllocation.minExecutors": "0",
                                    "spark.kubernetes.driver.podTemplateFile": f"s3://{cluster_name}-emr-eks-assets-{self.account}-{self.region}/{cluster_name}/pod-template/notebook-driver.yaml",
                                    "spark.kubernetes.executor.podTemplateFile": f"s3://{cluster_name}-emr-eks-assets-{self.account}-{self.region}/{cluster_name}/pod-template/notebook-executor.yaml",
                                    "spark.dynamicAllocation.enabled": "true",
                                    "spark.dynamicAllocation.executorAllocationRatio": "1"
                                }
                            },
                            {
                                "classification": "jupyter-kernel-overrides",
                                "configurations": [
                                    {
                                        "classification": "spark-python-kubernetes",
                                        "properties": {
                                            "container-image": f"{self.account}.dkr.ecr.{self.region}.amazonaws.com/me6.7_custom_repo:latest"
                                        }
                                    }
                                ] 
                            }
                        ],
                        "monitoringConfiguration": {
                            "persistentAppUI": "ENABLED",
                            "cloudWatchMonitoringConfiguration": {
                                "logGroupName": "/aws/emr-containers/notebook",
                                "logStreamNamePrefix": "default"
                            }
                        }
                    }

                )]
            )]
        )
