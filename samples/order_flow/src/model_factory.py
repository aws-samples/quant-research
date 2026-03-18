# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: MIT-0

"""
Model factory for creating different neural network architectures.
"""

import torch
import torch.nn as nn
import torch._dynamo
import logging
from typing import Dict, Any

import models

# Disable torch.compile errors and fall back to eager execution
# This fixes PyTorch inductor backend compilation issues
torch._dynamo.config.suppress_errors = True

logger = logging.getLogger(__name__)


class ModelFactory:
    """Factory class for creating different model architectures."""

    def __init__(self, num_features: int, sequence_length: int, num_classes: int = 3):
        self.num_features = num_features
        self.sequence_length = sequence_length
        self.num_classes = num_classes

    def create_model(self, model_name: str, **model_config) -> nn.Module:
        """Create a model by name with given configuration."""
        logger.info(f"Creating {model_name} model...")

        # Default model parameters
        hidden_size = model_config.get("hidden_size", 128)
        num_layers = model_config.get("num_layers", 2)
        dropout_rate = model_config.get("dropout_rate", 0.2)

        if model_name == "RNN":
            return models.RNNModel(
                input_size=self.num_features,
                hidden_size=hidden_size,
                num_layers=num_layers,
                num_classes=self.num_classes,
                dropout_rate=dropout_rate,
            )

        elif model_name == "CNN_LSTM":
            return models.CNN_LSTM(
                input_size=self.num_features,
                hidden_size=hidden_size,
                num_layers=num_layers,
                num_classes=self.num_classes,
            )

        elif model_name == "TFT":
            return models.TemporalFusionTransformer(
                num_features=self.num_features,
                num_static_features=min(5, self.num_features),
                hidden_size=hidden_size,
                num_heads=model_config.get("num_heads", 4),
                num_classes=self.num_classes,
                dropout=model_config.get("dropout", 0.1),
            )

        elif model_name == "OrderFlow_Transformer":
            return models.OrderFlowTransformer(
                num_features=self.num_features,
                d_model=hidden_size,
                n_heads=model_config.get("n_heads", 8),
                n_layers=model_config.get("n_layers", 4),
                d_ff=hidden_size * 4,
                max_seq_length=model_config.get("max_seq_length", 512),
                num_classes=self.num_classes,
                dropout=model_config.get("dropout", 0.1),
            )

        else:
            raise ValueError(
                f"Unknown model name: {model_name}. Available models: RNN, CNN_LSTM, TFT, OrderFlow_Transformer"
            )

    def get_model_info(self, model_name: str) -> Dict[str, Any]:
        """Get information about a specific model architecture."""
        model_info = {
            "RNN": {
                "description": "Simple RNN with LSTM/GRU layers",
                "parameters": ["hidden_size", "num_layers", "dropout_rate"],
                "suitable_for": "Basic sequence modeling",
            },
            "CNN_LSTM": {
                "description": "Convolutional Neural Network + LSTM hybrid",
                "parameters": ["hidden_size", "num_layers"],
                "suitable_for": "Feature extraction + sequence modeling",
            },
            "TFT": {
                "description": "Temporal Fusion Transformer for time series",
                "parameters": ["hidden_size", "num_heads", "dropout"],
                "suitable_for": "Complex temporal patterns with attention",
            },
            "OrderFlow_Transformer": {
                "description": "Specialized transformer for order flow data",
                "parameters": ["hidden_size", "n_heads", "n_layers", "dropout"],
                "suitable_for": "High-frequency trading data with complex dependencies",
            },
        }

        return model_info.get(
            model_name,
            {
                "description": "Unknown model",
                "parameters": [],
                "suitable_for": "Unknown",
            },
        )

    def list_available_models(self) -> list:
        """List all available model architectures."""
        return ["RNN", "CNN_LSTM", "TFT", "OrderFlow_Transformer"]


def create_model(
    model_name: str,
    num_features: int,
    sequence_length: int,
    num_classes: int = 3,
    **model_config,
) -> nn.Module:
    """Convenience function to create a model without instantiating ModelFactory."""
    factory = ModelFactory(num_features, sequence_length, num_classes)
    return factory.create_model(model_name, **model_config)


def get_model_forward_pass(model: nn.Module, inputs):
    """Handle forward pass for different model types with fallback."""
    try:
        model_type = str(type(model))

        if "OrderFlowTransformer" in model_type:
            outputs, aux_outputs = model(inputs)
            return outputs
        elif "TemporalFusionTransformer" in model_type:
            # For TFT, extract static features from first timestep
            static_features = inputs[:, 0, :5]
            outputs = model(inputs, static_features)
            return outputs
        else:
            # Standard forward pass for RNN, CNN_LSTM, etc.
            outputs = model(inputs)
            return outputs

    except Exception as e:
        logger.warning(
            f"Model forward pass error: {e}, using fallback standard forward pass"
        )
        return model(inputs)
