# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: MIT-0

import aws_cdk as core
from aws_cdk.assertions import Template

from infrastructure.common.pipeline import PipelineConfig, DeploymentPipelineStack


def test_pipeline_created():
    app = core.App()
    env = core.Environment(account="012345678901", region="us-east-1")

    pipeline_config = PipelineConfig(
        namespace="infrastructure",
        github_owner="github_owner",
        github_repo="github_repo",
        github_branch="main",
        github_token_secret_name="github-token",
        enable_code_pipeline=True,
    )

    deployment_pipeline = DeploymentPipelineStack(
        app,
        "infrastructure",
        env=env,
        config=pipeline_config,
    )
    template = Template.from_stack(deployment_pipeline)

    template.resource_count_is("AWS::ECR::Repository", 1)
    template.resource_count_is("AWS::CodeBuild::Project", 1)
    template.resource_count_is("AWS::CodePipeline::Pipeline", 1)
