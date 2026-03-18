# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: MIT-0

"""
Utility functions for memory management and common operations.
"""

import gc
import psutil
import logging
import torch

logger = logging.getLogger(__name__)


def log_memory_usage(stage: str = ""):
    """Log current memory usage for monitoring."""
    try:
        process = psutil.Process()
        memory_info = process.memory_info()
        memory_mb = memory_info.rss / 1024 / 1024
        logger.info(f"Memory usage {stage}: {memory_mb:.1f} MB")

        # Log GPU memory if CUDA is available
        if torch.cuda.is_available():
            gpu_memory = torch.cuda.memory_allocated() / 1024 / 1024
            logger.info(f"GPU memory usage {stage}: {gpu_memory:.1f} MB")
    except Exception as e:
        logger.warning(f"Could not log memory usage: {e}")


def force_garbage_collection():
    """Force garbage collection and clear caches."""
    gc.collect()
    if torch.cuda.is_available():
        torch.cuda.empty_cache()
    logger.debug("Forced garbage collection and cache clearing")


def get_model_components():
    """Import and return model creation and forward pass functions."""
    from model_factory import create_model, get_model_forward_pass

    return create_model, get_model_forward_pass
