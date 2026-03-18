#!/usr/bin/env python3
import aws_cdk as cdk
from aws_cdk import (
    Stack,
    aws_codebuild as codebuild,
    aws_codecommit as codecommit,
    aws_ecr as ecr,
    aws_iam as iam,
    aws_events as events,
    aws_events_targets as targets,
)
from constructs import Construct

class RayDockerPipelineStack(Stack):
    def __init__(self, scope: Construct, construct_id: str, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

        # Reference existing ECR repository
        ecr_repo = ecr.Repository.from_repository_name(
            self, "ECRRepo", "ray_anyscale_custom"
        )

        # Reference existing CodeCommit repository
        repo = codecommit.Repository.from_repository_name(
            self, "CodeCommitRepo", 
            "quant-research-sample-using-amazon-ecs-and-aws-batch"
        )

        # CodeBuild project
        build_project = codebuild.Project(
            self, "RayDockerBuild",
            project_name="ray-docker-build",
            source=codebuild.Source.code_commit(
                repository=repo,
                branch_or_ref="main"
            ),
            environment=codebuild.BuildEnvironment(
                build_image=codebuild.LinuxBuildImage.STANDARD_7_0,
                privileged=True  # Required for Docker builds
            ),
            build_spec=codebuild.BuildSpec.from_object({
                "version": "0.2",
                "phases": {
                    "pre_build": {
                        "commands": [
                            "echo Logging in to Amazon ECR...",
                            "aws ecr get-login-password --region $AWS_DEFAULT_REGION | docker login --username AWS --password-stdin $ECR_REPOSITORY_URI"
                        ]
                    },
                    "build": {
                        "commands": [
                            "echo Build started on `date`",
                            "cd samples/order_flow_ray/ray_infrastructure",
                            "cp -r ../src .",
                            "docker build --platform linux/arm64 -t $IMAGE_REPO_NAME:$IMAGE_TAG .",
                            "docker tag $IMAGE_REPO_NAME:$IMAGE_TAG $ECR_REPOSITORY_URI:$IMAGE_TAG"
                        ]
                    },
                    "post_build": {
                        "commands": [
                            "echo Build completed on `date`",
                            "echo Pushing the Docker image...",
                            "docker push $ECR_REPOSITORY_URI:$IMAGE_TAG"
                        ]
                    }
                },
                "env": {
                    "variables": {
                        "IMAGE_REPO_NAME": "ray_anyscale_custom",
                        "IMAGE_TAG": "latest",
                        "ECR_REPOSITORY_URI": ecr_repo.repository_uri
                    }
                }
            })
        )

        # Grant ECR permissions
        ecr_repo.grant_pull_push(build_project)

        # CloudWatch Events rule for CodeCommit changes
        rule = events.Rule(
            self, "CodeCommitRule",
            event_pattern=events.EventPattern(
                source=["aws.codecommit"],
                detail_type=["CodeCommit Repository State Change"],
                detail={
                    "repositoryName": [repo.repository_name],
                    "referenceType": ["branch"],
                    "referenceName": ["main"]
                }
            )
        )

        # Add CodeBuild as target
        rule.add_target(targets.CodeBuildProject(build_project))

app = cdk.App()
RayDockerPipelineStack(app, "RayDockerPipelineStack")
app.synth()