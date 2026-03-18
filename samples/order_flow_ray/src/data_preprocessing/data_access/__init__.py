# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: MIT-0

from .base import DataAccess
from .s3 import S3DataAccess
from .s3tables import S3TablesDataAccess
from .factory import DataAccessFactory

__all__ = ["DataAccess", "S3DataAccess", "S3TablesDataAccess", "DataAccessFactory"]