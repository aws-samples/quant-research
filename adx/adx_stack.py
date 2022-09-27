from aws_cdk import (
    Stack,
    Aws,
    ArnFormat
)

from aws_cdk.aws_iam import (
    ManagedPolicy,
    PolicyStatement,
)

from constructs import Construct

from aws_analytics_reference_architecture import (
        EmrEksCluster,
        NotebookPlatform,
        StudioAuthMode,
        SSOIdentityType,
        NotebookUserOptions, 
        NotebookManagedEndpointOptions,
)


class AdxStack(Stack):

    def __init__(self, scope: Construct, construct_id: str, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

        emr_eks = EmrEksCluster.get_or_create(self,
        eks_admin_role_arn="arn:aws:iam::318535617973:role/AdminAccess",
        eks_cluster_name="clusteradx"
        )

        
        notebook_platform = NotebookPlatform(self, "platform-notebook",
            emr_eks=emr_eks,
            eks_namespace="platformns",
            studio_name="platform",
            studio_auth_mode=StudioAuthMode.SSO
        )

        # If the S3 bucket is encrypted, add policy to the key for the role
        policy1 = ManagedPolicy(self, "MyPolicy1",
            statements=[
                PolicyStatement(
                    resources=["arn:aws:s3:::alexvt-emr-eks","arn:aws:s3:::clusteradx-emr-eks-assets-318535617973-us-east-1"] ,
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
                identity_name="emrstudio", # make sure user already setup in AWS SSO 
                identity_type= SSOIdentityType.USER.value,
                notebook_managed_endpoints = [NotebookManagedEndpointOptions(
                    emr_on_eks_version = "emr-6.7.0-latest",
                    managed_endpoint_name = "ManagedEndpoint1",
                    execution_policy = policy1
                )]
            )]
        )
