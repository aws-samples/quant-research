# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: MIT-0

import torch
import torch.nn as nn
import torch.nn.functional as F
from torch.autograd import Variable
from typing import Optional, Tuple
import math


class RNNModel(nn.Module):
    """Simple RNN model for sequence data."""

    def __init__(
        self, input_size, hidden_size, num_layers, num_classes, dropout_rate=0.2
    ):
        super(RNNModel, self).__init__()
        # Number of hidden dimensions
        self.hidden_dim = hidden_size
        # Number of hidden layers
        self.layer_dim = num_layers
        # RNN
        self.rnn = nn.RNN(
            input_size,
            hidden_size,
            num_layers,
            batch_first=True,
            nonlinearity="relu",
            dropout=dropout_rate,
        )
        self.dropout = nn.Dropout(dropout_rate)
        # Readout layer
        self.fc = nn.Linear(hidden_size, num_classes)

    def forward(self, x):
        # Initialize hidden state with zeros
        h0 = Variable(
            torch.zeros(self.layer_dim, x.size(0), self.hidden_dim).to(x.device)
        )
        # One time step
        out, hn = self.rnn(x, h0)
        out = self.dropout(out)
        out = self.fc(out[:, -1, :])
        return out


class CNN_LSTM(nn.Module):
    """CNN-LSTM hybrid model for sequence data."""

    def __init__(self, input_size, hidden_size, num_layers, num_classes):
        super(CNN_LSTM, self).__init__()
        self.cnn = nn.Sequential(
            nn.Conv1d(
                in_channels=input_size,
                out_channels=64,
                kernel_size=3,
                stride=1,
                padding=1,
            ),
            nn.ReLU(),
            nn.MaxPool1d(kernel_size=2, stride=2),
            nn.Conv1d(
                in_channels=64, out_channels=128, kernel_size=3, stride=1, padding=1
            ),
            nn.ReLU(),
            nn.MaxPool1d(kernel_size=2, stride=2),
        )
        self.lstm = nn.LSTM(
            input_size=128,
            hidden_size=hidden_size,
            num_layers=num_layers,
            batch_first=True,
        )
        self.fc = nn.Linear(hidden_size, num_classes)

    def forward(self, x):
        # CNN takes input of shape (batch_size, channels, seq_len)
        x = x.permute(0, 2, 1)
        out = self.cnn(x)
        # LSTM takes input of shape (batch_size, seq_len, input_size)
        out = out.permute(0, 2, 1)
        out, _ = self.lstm(out)
        out = self.fc(out[:, -1, :])
        return out


class VariableSelectionNetwork(nn.Module):
    """Selects relevant features using gating mechanism."""

    def __init__(self, input_size: int, hidden_size: int, dropout: float = 0.1):
        super().__init__()
        self.flattened_grn = GatedResidualNetwork(input_size, hidden_size, dropout)
        self.softmax = nn.Softmax(dim=-1)

    def forward(self, x: torch.Tensor) -> torch.Tensor:
        # Flatten temporal dimension if present
        original_shape = x.shape
        if len(x.shape) == 3:
            x = x.view(-1, x.shape[-1])

        # Apply GRN
        weights = self.flattened_grn(x)
        weights = self.softmax(weights)

        # Apply weights
        output = x * weights

        # Reshape back if needed
        if len(original_shape) == 3:
            output = output.view(original_shape[0], original_shape[1], -1)

        return output


class GatedResidualNetwork(nn.Module):
    """Gated Residual Network for feature processing."""

    def __init__(self, input_size: int, hidden_size: int, dropout: float = 0.1):
        super().__init__()
        self.layer1 = nn.Linear(input_size, hidden_size)
        self.layer2 = nn.Linear(hidden_size, hidden_size)

        self.gate = nn.Sequential(nn.Linear(hidden_size, hidden_size), nn.Sigmoid())

        self.layer_norm = nn.LayerNorm(hidden_size)
        self.dropout = nn.Dropout(dropout)

    def forward(
        self, x: torch.Tensor, context: Optional[torch.Tensor] = None
    ) -> torch.Tensor:
        # Initial transformation
        residual = self.layer1(x)

        # Apply context if provided
        if context is not None:
            residual = residual + context

        # Gated transformation
        temp = F.elu(residual)
        gate = self.gate(temp)
        temp = self.layer2(temp)

        # Apply gate and residual connection
        output = gate * temp + (1 - gate) * residual
        output = self.layer_norm(output)
        output = self.dropout(output)

        return output


class TemporalFusionTransformer(nn.Module):
    """
    Temporal Fusion Transformer adapted for order flow prediction.
    Based on "Temporal Fusion Transformers for Interpretable Multi-horizon Time Series Forecasting"
    """

    def __init__(
        self,
        num_features: int,
        num_static_features: int = 5,
        hidden_size: int = 128,
        num_heads: int = 4,
        num_classes: int = 3,
        dropout: float = 0.1,
    ):
        super().__init__()

        # Variable selection networks
        self.static_variable_selection = VariableSelectionNetwork(
            num_static_features, hidden_size, dropout
        )
        self.temporal_variable_selection = VariableSelectionNetwork(
            num_features, hidden_size, dropout
        )

        # Static covariate encoder
        self.static_encoder = nn.Sequential(
            nn.Linear(num_static_features, hidden_size),
            nn.ReLU(),
            nn.Linear(hidden_size, hidden_size),
        )

        # LSTM for temporal processing
        self.lstm = nn.LSTM(
            hidden_size, hidden_size, batch_first=True, bidirectional=True
        )

        # Multi-head attention for temporal relationships
        self.temporal_attention = nn.MultiheadAttention(
            hidden_size * 2, num_heads, dropout=dropout
        )

        # Gated residual networks
        self.grn1 = GatedResidualNetwork(hidden_size * 2, hidden_size, dropout)
        self.grn2 = GatedResidualNetwork(hidden_size, hidden_size, dropout)

        # Output layer
        self.output_layer = nn.Sequential(
            nn.Linear(hidden_size, hidden_size // 2),
            nn.ReLU(),
            nn.Dropout(dropout),
            nn.Linear(hidden_size // 2, num_classes),
        )

    def forward(
        self, temporal_features: torch.Tensor, static_features: torch.Tensor
    ) -> torch.Tensor:
        # Variable selection
        temporal_features = self.temporal_variable_selection(temporal_features)
        static_features = self.static_variable_selection(static_features)

        # Encode static features
        static_encoded = self.static_encoder(static_features)

        # Process temporal features with LSTM
        lstm_out, _ = self.lstm(temporal_features)

        # Apply temporal attention
        attn_out, _ = self.temporal_attention(
            lstm_out.transpose(0, 1), lstm_out.transpose(0, 1), lstm_out.transpose(0, 1)
        )
        attn_out = attn_out.transpose(0, 1)

        # Combine with static features
        combined = self.grn1(attn_out, static_encoded.unsqueeze(1))

        # Final processing
        output = self.grn2(combined[:, -1, :])  # Use last time step

        return self.output_layer(output)


class PositionalEncoding(nn.Module):
    """Positional encoding for transformer models."""

    def __init__(self, d_model: int, max_len: int = 5000):
        super().__init__()
        pe = torch.zeros(max_len, d_model)
        position = torch.arange(0, max_len, dtype=torch.float).unsqueeze(1)
        div_term = torch.exp(
            torch.arange(0, d_model, 2).float() * (-math.log(10000.0) / d_model)
        )
        pe[:, 0::2] = torch.sin(position * div_term)
        pe[:, 1::2] = torch.cos(position * div_term)
        self.register_buffer("pe", pe.unsqueeze(0).transpose(0, 1))

    def forward(self, x: torch.Tensor) -> torch.Tensor:
        return x + self.pe[: x.size(0), :]


class MultiHeadAttentionWithContext(nn.Module):
    """Multi-head attention that incorporates market context."""

    def __init__(self, d_model: int, n_heads: int, dropout: float = 0.1):
        super().__init__()
        self.d_model = d_model
        self.n_heads = n_heads
        self.d_k = d_model // n_heads

        self.W_q = nn.Linear(d_model, d_model)
        self.W_k = nn.Linear(d_model, d_model)
        self.W_v = nn.Linear(d_model, d_model)
        self.W_o = nn.Linear(d_model, d_model)

        # Additional context projection for market conditions
        self.context_projection = nn.Linear(d_model, d_model)

        self.dropout = nn.Dropout(dropout)
        self.layer_norm = nn.LayerNorm(d_model)

    def forward(
        self,
        query: torch.Tensor,
        key: torch.Tensor,
        value: torch.Tensor,
        mask: Optional[torch.Tensor] = None,
        market_context: Optional[torch.Tensor] = None,
    ) -> torch.Tensor:
        batch_size = query.size(0)
        seq_len = query.size(1)

        # Linear transformations and split into heads
        Q = (
            self.W_q(query)
            .view(batch_size, seq_len, self.n_heads, self.d_k)
            .transpose(1, 2)
        )
        K = (
            self.W_k(key)
            .view(batch_size, seq_len, self.n_heads, self.d_k)
            .transpose(1, 2)
        )
        V = (
            self.W_v(value)
            .view(batch_size, seq_len, self.n_heads, self.d_k)
            .transpose(1, 2)
        )

        # Incorporate market context if provided
        if market_context is not None:
            context = self.context_projection(market_context)
            K = K + context.view(batch_size, 1, self.n_heads, self.d_k).expand_as(K)

        # Attention scores
        scores = torch.matmul(Q, K.transpose(-2, -1)) / math.sqrt(self.d_k)

        if mask is not None:
            scores = scores.masked_fill(mask == 0, -1e9)

        attn_weights = F.softmax(scores, dim=-1)
        attn_weights = self.dropout(attn_weights)

        # Apply attention to values
        context = torch.matmul(attn_weights, V)
        context = (
            context.transpose(1, 2).contiguous().view(batch_size, seq_len, self.d_model)
        )

        output = self.W_o(context)
        output = self.dropout(output)
        output = self.layer_norm(output + query)

        return output


class TransformerEncoderLayerWithCrossAttention(nn.Module):
    """Enhanced transformer encoder layer with cross-attention capabilities."""

    def __init__(self, d_model: int, n_heads: int, d_ff: int, dropout: float = 0.1):
        super().__init__()

        # Self-attention
        self.self_attn = MultiHeadAttentionWithContext(d_model, n_heads, dropout)

        # Feed-forward network
        self.ffn = nn.Sequential(
            nn.Linear(d_model, d_ff),
            nn.GELU(),  # GELU often works better than ReLU for transformers
            nn.Dropout(dropout),
            nn.Linear(d_ff, d_model),
        )

        # Layer normalization
        self.norm1 = nn.LayerNorm(d_model)
        self.norm2 = nn.LayerNorm(d_model)

        # Dropout
        self.dropout = nn.Dropout(dropout)

    def forward(
        self, x: torch.Tensor, mask: Optional[torch.Tensor] = None
    ) -> Tuple[torch.Tensor, torch.Tensor]:
        # Self-attention with residual connection
        attn_output = self.self_attn(x, x, x, mask)
        x = self.norm1(x + self.dropout(attn_output))

        # Feed-forward with residual connection
        ffn_output = self.ffn(x)
        x = self.norm2(x + self.dropout(ffn_output))

        return x, attn_output


class OrderFlowTransformer(nn.Module):
    """
    State-of-the-art transformer architecture for order flow prediction.
    Incorporates temporal patterns, cross-feature attention, and market regime awareness.
    """

    def __init__(
        self,
        num_features: int,
        d_model: int = 256,
        n_heads: int = 8,
        n_layers: int = 6,
        d_ff: int = 1024,
        max_seq_length: int = 512,
        num_classes: int = 3,
        dropout: float = 0.1,
        use_temporal_conv: bool = True,
        use_mixup: bool = True,
    ):
        super().__init__()

        self.d_model = d_model
        self.use_temporal_conv = use_temporal_conv
        self.use_mixup = use_mixup

        # Input projection with optional temporal convolution
        if use_temporal_conv:
            # Temporal convolution to capture local patterns
            self.temporal_conv = nn.Sequential(
                nn.Conv1d(num_features, d_model // 2, kernel_size=3, padding=1),
                nn.ReLU(),
                nn.Conv1d(d_model // 2, d_model // 2, kernel_size=3, padding=1),
                nn.ReLU(),
                nn.Conv1d(d_model // 2, d_model, kernel_size=3, padding=1),
                nn.LayerNorm([d_model]),
            )
            self.feature_projection = nn.Linear(num_features, d_model)
            self.fusion_gate = nn.Sequential(
                nn.Linear(d_model * 2, d_model), nn.Sigmoid()
            )
        else:
            self.input_projection = nn.Linear(num_features, d_model)

        # Positional encoding
        self.positional_encoding = PositionalEncoding(d_model, max_seq_length)

        # Transformer encoder layers
        self.transformer_layers = nn.ModuleList(
            [
                TransformerEncoderLayerWithCrossAttention(
                    d_model=d_model, n_heads=n_heads, d_ff=d_ff, dropout=dropout
                )
                for _ in range(n_layers)
            ]
        )

        # Market regime embedding
        self.regime_embedding = nn.Embedding(
            4, d_model
        )  # 4 regimes: normal, volatile, trending, mean-reverting

        # Output layers
        self.output_attention = nn.MultiheadAttention(d_model, n_heads, dropout=dropout)
        self.output_projection = nn.Sequential(
            nn.Linear(d_model, d_model // 2),
            nn.ReLU(),
            nn.Dropout(dropout),
            nn.Linear(d_model // 2, num_classes),
        )

        # Auxiliary outputs for multi-task learning
        self.volatility_prediction = nn.Linear(d_model, 1)
        self.volume_prediction = nn.Linear(d_model, 1)

        self.dropout = nn.Dropout(dropout)

    def detect_market_regime(self, x: torch.Tensor) -> torch.Tensor:
        """Detect market regime from input features."""
        # Simple regime detection based on volatility and trend
        # In practice, this could be more sophisticated
        volatility = x[:, :, -1].std(dim=1)  # Assuming last feature is volatility
        trend = (x[:, -1, 0] - x[:, 0, 0]) / (x[:, 0, 0] + 1e-8)

        regime = torch.zeros(x.size(0), dtype=torch.long, device=x.device)
        regime[volatility > volatility.median()] = 1  # High volatility
        regime[trend.abs() > 0.02] = 2  # Trending
        regime[(volatility < volatility.median()) & (trend.abs() < 0.01)] = (
            3  # Mean-reverting
        )

        return regime

    def forward(
        self,
        x: torch.Tensor,
        mask: Optional[torch.Tensor] = None,
        return_attention: bool = False,
    ) -> Tuple[torch.Tensor, dict]:
        batch_size, seq_len, _ = x.size()

        # Input processing
        if self.use_temporal_conv:
            # Temporal convolution path
            x_conv = self.temporal_conv(x.transpose(1, 2)).transpose(1, 2)

            # Feature projection path
            x_proj = self.feature_projection(x)

            # Gated fusion
            gate = self.fusion_gate(torch.cat([x_conv, x_proj], dim=-1))
            x = gate * x_conv + (1 - gate) * x_proj
        else:
            x = self.input_projection(x)

        # Add positional encoding
        x = self.positional_encoding(x)

        # Detect and embed market regime
        regime = self.detect_market_regime(x)
        regime_emb = self.regime_embedding(regime).unsqueeze(1).expand(-1, seq_len, -1)
        x = x + regime_emb * 0.1  # Small contribution

        # Apply transformer layers
        attention_weights = []
        for layer in self.transformer_layers:
            x, attn = layer(x, mask=mask)
            if return_attention:
                attention_weights.append(attn)

        # Global attention pooling for final prediction
        # Use last position as query for attention over all positions
        query = x[:, -1:, :]  # Last time step
        attended_output, final_attn = self.output_attention(query, x, x, attn_mask=mask)

        # Final predictions
        output = self.output_projection(attended_output.squeeze(1))

        # Auxiliary predictions
        volatility_pred = self.volatility_prediction(x[:, -1, :])
        volume_pred = self.volume_prediction(x[:, -1, :])

        aux_outputs = {
            "volatility": volatility_pred,
            "volume": volume_pred,
            "attention_weights": attention_weights if return_attention else None,
            "regime": regime,
        }

        return output, aux_outputs
