# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: MIT-0

import json
import os
import re
from dataclasses import dataclass
from functools import lru_cache
from types import SimpleNamespace
from typing import Union, List, Dict, Any

from aws_cdk import App
from dotenv import load_dotenv


@dataclass
class EnvironmentConfig:
    """Environment configuration container"""

    aws_account_id: str
    aws_region: str
    namespace: str
    github_owner: str
    github_repo: str
    github_token: str
    github_branch: str

    @classmethod
    def from_env(cls) -> "EnvironmentConfig":
        """Create configuration from environment variables"""
        load_dotenv()
        return cls(
            aws_account_id=os.getenv("AWS_ACCOUNT_ID"),
            aws_region=os.getenv("AWS_REGION"),
            namespace=os.getenv("NAMESPACE", "quant-research-with-aws-batch"),
            github_owner=os.getenv("GITHUB_OWNER"),
            github_repo=os.getenv("GITHUB_REPO"),
            github_token=os.getenv("GITHUB_TOKEN_SECRET_NAME"),
            github_branch=os.getenv("GITHUB_BRANCH", "main"),
        )


@lru_cache()
def load_parameters(app: App) -> Union[SimpleNamespace, List, Any]:
    """Load and cache parameters from file or context"""
    with open("config/parameters.json", "r") as f:
        default_parameters = json.load(f)

    # Get parameters from context if available, otherwise use defaults
    raw_params = app.node.try_get_context("parameters") or default_parameters

    # Convert the dictionary to an object with attributes
    return dict_to_obj(raw_params)


def dict_to_obj(d: Union[Dict, List, Any]) -> Union[SimpleNamespace, List, Any]:
    """
    Convert a dictionary to a SimpleNamespace object recursively.

    Args:
        d: Input dictionary, list, or primitive type

    Returns:
        SimpleNamespace object, list, or primitive type
    """
    # Handle None case early
    if d is None:
        return None

    # Use type checking with direct references to improve performance
    d_type = type(d)

    if d_type is dict:
        # Pre-calculate the dictionary items to avoid multiple iterations
        return SimpleNamespace(**{k: dict_to_obj(v) for k, v in d.items()})

    if d_type is list:
        # Use list comprehension for better performance than map()
        return [dict_to_obj(v) for v in d]

    return d


def get_stack_name(namespace: str, prefix: str) -> str:
    """
    Get CloudFormation stack name by adding prefix and converting spaces to hyphens.

    Args:
        namespace (str): The input namespace
        prefix (str): Prefix to add

    Returns:
        str: Formatted namespace string, truncated to 128 characters if needed
    """
    if not namespace:
        return prefix.rstrip("-")

    # Use regex to handle all whitespace and multiple hyphens in one pass
    formatted = re.sub(r"[-\s]+", "-", namespace.lower().strip())

    # Combine prefix addition and truncation in one step
    result = f"{prefix}{formatted}" if not formatted.startswith(prefix) else formatted
    return result[:128].rstrip("-")
