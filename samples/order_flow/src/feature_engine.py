# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: MIT-0

"""
Feature engineering module using Polars for memory-efficient computation.
"""

import numpy as np
import polars as pl
import logging
from typing import List

logger = logging.getLogger(__name__)


class PolarsFeatureEngine:
    """Memory-efficient feature computation engine using Polars."""

    def __init__(self):
        self.computed_features = []

    def rolling_mean(
        self, df: pl.DataFrame, column: str, window: int, alias: str = None
    ) -> pl.Expr:
        """Rolling mean using Polars."""
        alias = alias or f"{column}_rolling_mean_{window}"
        return pl.col(column).rolling_mean(window_size=window).alias(alias)

    def rolling_std(
        self, df: pl.DataFrame, column: str, window: int, alias: str = None
    ) -> pl.Expr:
        """Rolling standard deviation using Polars."""
        alias = alias or f"{column}_rolling_std_{window}"
        return pl.col(column).rolling_std(window_size=window).alias(alias)

    def rolling_sum(
        self, df: pl.DataFrame, column: str, window: int, alias: str = None
    ) -> pl.Expr:
        """Rolling sum using Polars."""
        alias = alias or f"{column}_rolling_sum_{window}"
        return pl.col(column).rolling_sum(window_size=window).alias(alias)

    def rolling_quantile(
        self,
        df: pl.DataFrame,
        column: str,
        window: int,
        quantile: float,
        alias: str = None,
    ) -> pl.Expr:
        """Rolling quantile using Polars."""
        alias = alias or f"{column}_rolling_q{int(quantile*100)}_{window}"
        return (
            pl.col(column)
            .rolling_quantile(quantile=quantile, window_size=window)
            .alias(alias)
        )

    def rolling_min(
        self, df: pl.DataFrame, column: str, window: int, alias: str = None
    ) -> pl.Expr:
        """Rolling minimum using Polars."""
        alias = alias or f"{column}_rolling_min_{window}"
        return pl.col(column).rolling_min(window_size=window).alias(alias)

    def rolling_max(
        self, df: pl.DataFrame, column: str, window: int, alias: str = None
    ) -> pl.Expr:
        """Rolling maximum using Polars."""
        alias = alias or f"{column}_rolling_max_{window}"
        return pl.col(column).rolling_max(window_size=window).alias(alias)

    def calculate_rsi(
        self, df: pl.DataFrame, price_col: str, period: int = 14, alias: str = None
    ) -> pl.Expr:
        """Calculate RSI using Polars expressions - simplified version."""
        alias = alias or f"rsi_{period}"

        # Simplified RSI calculation using direct expressions
        price_diff = pl.col(price_col).diff()

        # Calculate gains and losses in one expression, then RSI
        gains = pl.when(price_diff > 0).then(price_diff).otherwise(0)
        losses = pl.when(price_diff < 0).then(-price_diff).otherwise(0)

        # Calculate average gains and losses, then RSI in one expression
        avg_gains = gains.rolling_mean(window_size=period)
        avg_losses = losses.rolling_mean(window_size=period)

        # Calculate RSI
        rs = avg_gains / (avg_losses + 1e-8)
        rsi = (100 - (100 / (1 + rs))).clip(0, 100).alias(alias)

        return rsi

    def compute_advanced_features(
        self, df: pl.DataFrame, window: int = 128
    ) -> pl.DataFrame:
        """Compute advanced market microstructure features using Polars."""
        logger.info(f"Computing advanced features with window size: {window}")

        # Ensure required columns exist
        required_cols = [
            "trade_price",
            "trade_quantity",
            "bid_price",
            "ask_price",
            "bid_quantity",
            "ask_quantity",
        ]
        missing_cols = [col for col in required_cols if col not in df.columns]

        if missing_cols:
            logger.warning(
                f"Missing columns: {missing_cols}. Creating synthetic columns."
            )
            # Create missing columns with reasonable defaults
            synthetic_exprs = []
            if "bid_quantity" not in df.columns:
                synthetic_exprs.extend(
                    [
                        (pl.col("trade_quantity") * 0.5).alias("bid_quantity"),
                        (pl.col("trade_quantity") * 0.5).alias("ask_quantity"),
                    ]
                )
            if synthetic_exprs:
                df = df.with_columns(synthetic_exprs)

        # Create basic derived features
        df = self._create_basic_features(df)

        # Create rolling features
        df = self._create_rolling_features(df, window)

        # Create advanced composite features
        df = self._create_composite_features(df, window)

        # Clean features
        df = self._clean_computed_features(df)

        logger.info(
            f"✓ Computed {len([col for col in df.columns if any(suffix in col for suffix in ['rolling', 'rsi', 'vpin', 'momentum', 'ratio'])])} advanced features"
        )

        return df

    def _create_basic_features(self, df: pl.DataFrame) -> pl.DataFrame:
        """Create basic derived features."""
        # First, create basic features including trade_direction
        df = df.with_columns(
            [
                # Basic derived features
                ((pl.col("bid_price") + pl.col("ask_price")) / 2).alias("mid_price"),
                (pl.col("ask_price") - pl.col("bid_price")).alias("spread"),
                (
                    pl.col("trade_price")
                    - (pl.col("bid_price") + pl.col("ask_price")) / 2
                ).alias("price_vs_mid"),
                (pl.col("trade_price").pct_change().clip(-0.05, 0.05)).alias("returns"),
                pl.col("trade_price").diff().alias("price_change"),
                (pl.col("trade_price") * pl.col("trade_quantity")).alias(
                    "dollar_volume"
                ),
                # Trade direction (Lee-Ready algorithm approximation)
                pl.when(
                    pl.col("trade_price")
                    > (pl.col("bid_price") + pl.col("ask_price")) / 2
                )
                .then(1)
                .when(
                    pl.col("trade_price")
                    < (pl.col("bid_price") + pl.col("ask_price")) / 2
                )
                .then(-1)
                .otherwise(0)
                .alias("trade_direction"),
                # Order book imbalance
                (
                    (pl.col("bid_quantity") - pl.col("ask_quantity"))
                    / (pl.col("bid_quantity") + pl.col("ask_quantity") + 1e-6)
                ).alias("book_imbalance"),
                # Microprice
                (
                    (
                        pl.col("bid_price") * pl.col("ask_quantity")
                        + pl.col("ask_price") * pl.col("bid_quantity")
                    )
                    / (pl.col("bid_quantity") + pl.col("ask_quantity") + 1e-6)
                ).alias("microprice"),
            ]
        )

        # Then, create volume-based features that reference trade_direction
        df = df.with_columns(
            [
                # Volume-based features (now trade_direction exists)
                (
                    pl.col("trade_quantity")
                    * pl.when(pl.col("trade_direction") == 1)
                    .then(pl.col("trade_quantity"))
                    .otherwise(0)
                ).alias("buy_volume"),
                (
                    pl.col("trade_quantity")
                    * pl.when(pl.col("trade_direction") == -1)
                    .then(pl.col("trade_quantity"))
                    .otherwise(0)
                ).alias("sell_volume"),
                (pl.col("trade_quantity") * pl.col("trade_direction")).alias(
                    "signed_volume"
                ),
            ]
        )

        return df

    def _create_rolling_features(self, df: pl.DataFrame, window: int) -> pl.DataFrame:
        """Create rolling window features."""
        feature_exprs = []

        # Price-based rolling features
        for col in ["trade_price", "mid_price", "returns"]:
            if col in df.columns:
                feature_exprs.extend(
                    [
                        self.rolling_mean(df, col, window),
                        self.rolling_std(df, col, window),
                        self.rolling_min(df, col, window // 2),
                        self.rolling_max(df, col, window // 2),
                    ]
                )

        # Volume-based rolling features
        for col in ["trade_quantity", "dollar_volume", "signed_volume"]:
            if col in df.columns:
                feature_exprs.extend(
                    [
                        self.rolling_mean(df, col, window),
                        self.rolling_std(df, col, window),
                        self.rolling_sum(df, col, window // 4),
                    ]
                )

        # Order book rolling features
        for col in ["book_imbalance", "spread"]:
            if col in df.columns:
                feature_exprs.extend(
                    [
                        self.rolling_mean(df, col, window),
                        self.rolling_std(df, col, window // 2),
                    ]
                )

        # Quantile-based features
        for col in ["trade_quantity", "dollar_volume"]:
            if col in df.columns:
                feature_exprs.extend(
                    [
                        self.rolling_quantile(df, col, window, 0.25),
                        self.rolling_quantile(df, col, window, 0.75),
                        self.rolling_quantile(df, col, window, 0.95),
                    ]
                )

        # Add RSI
        feature_exprs.append(self.calculate_rsi(df, "trade_price", 14))

        # Apply all rolling calculations at once (more efficient)
        logger.info(f"Computing {len(feature_exprs)} rolling features...")
        return df.with_columns(feature_exprs)

    def _create_composite_features(self, df: pl.DataFrame, window: int) -> pl.DataFrame:
        """Create advanced composite features."""
        logger.info("Computing advanced composite features...")
        composite_exprs = []

        # VPIN approximation
        composite_exprs.append(
            (
                pl.col("signed_volume").abs().rolling_sum(window_size=window)
                / pl.col("trade_quantity")
                .rolling_sum(window_size=window)
                .clip(lower_bound=1e-6)
            ).alias("vpin")
        )

        # Price momentum
        price_mean_col = f"trade_price_rolling_mean_{window}"
        if price_mean_col in df.columns:
            composite_exprs.append(
                ((pl.col("trade_price") / pl.col(price_mean_col)) - 1).alias(
                    "price_momentum"
                )
            )
        else:
            composite_exprs.append(
                (
                    (
                        pl.col("trade_price")
                        / pl.col("trade_price").rolling_mean(window_size=window)
                    )
                    - 1
                ).alias("price_momentum")
            )

        # Volume ratio
        volume_mean_col = f"trade_quantity_rolling_mean_{window}"
        if volume_mean_col in df.columns:
            composite_exprs.append(
                (
                    pl.col("trade_quantity")
                    / pl.col(volume_mean_col).clip(lower_bound=1e-6)
                ).alias("volume_ratio")
            )
        else:
            composite_exprs.append(
                (
                    pl.col("trade_quantity")
                    / pl.col("trade_quantity")
                    .rolling_mean(window_size=window)
                    .clip(lower_bound=1e-6)
                ).alias("volume_ratio")
            )

        # Volatility ratio
        returns_std_short_col = f"returns_rolling_std_{window//4}"
        returns_std_long_col = f"returns_rolling_std_{window}"

        if returns_std_short_col in df.columns and returns_std_long_col in df.columns:
            composite_exprs.append(
                (
                    pl.col(returns_std_short_col)
                    / pl.col(returns_std_long_col).clip(lower_bound=1e-8)
                ).alias("volatility_ratio")
            )
        else:
            composite_exprs.append(
                (
                    pl.col("returns").rolling_std(window_size=window // 4)
                    / pl.col("returns")
                    .rolling_std(window_size=window)
                    .clip(lower_bound=1e-8)
                ).alias("volatility_ratio")
            )

        # Relative spread
        composite_exprs.append(
            (pl.col("spread") / pl.col("mid_price").clip(lower_bound=1e-6)).alias(
                "relative_spread"
            )
        )

        # Price vs VWAP approximation
        dollar_sum_col = f"dollar_volume_rolling_sum_{window}"
        quantity_sum_col = f"trade_quantity_rolling_sum_{window}"

        if dollar_sum_col in df.columns and quantity_sum_col in df.columns:
            composite_exprs.append(
                (
                    (
                        pl.col("trade_price")
                        - (
                            pl.col(dollar_sum_col)
                            / pl.col(quantity_sum_col).clip(lower_bound=1e-6)
                        )
                    )
                    / pl.col("trade_price").clip(lower_bound=1e-6)
                ).alias("price_vs_vwap")
            )
        else:
            composite_exprs.append(
                (
                    (
                        pl.col("trade_price")
                        - (
                            pl.col("dollar_volume").rolling_sum(window_size=window)
                            / pl.col("trade_quantity")
                            .rolling_sum(window_size=window)
                            .clip(lower_bound=1e-6)
                        )
                    )
                    / pl.col("trade_price").clip(lower_bound=1e-6)
                ).alias("price_vs_vwap")
            )

        return df.with_columns(composite_exprs)

    def _clean_computed_features(self, df: pl.DataFrame) -> pl.DataFrame:
        """Clean computed features - handle NaN/inf values."""
        logger.info("Cleaning computed features...")

        # Get all numeric columns for cleaning
        numeric_cols = []
        for col in df.columns:
            if df[col].dtype in [pl.Float32, pl.Float64, pl.Int32, pl.Int64]:
                numeric_cols.append(col)

        # Fill NaN/inf values for derived features only
        clean_exprs = []
        for col in numeric_cols:
            if col.startswith(
                (
                    "trade_price",
                    "returns",
                    "volume",
                    "book",
                    "spread",
                    "rsi",
                    "vpin",
                    "momentum",
                    "ratio",
                )
            ):
                clean_exprs.append(
                    pl.col(col)
                    .fill_nan(0.0)
                    .map_elements(
                        lambda x: (
                            0.0 if (x == float("inf") or x == float("-inf")) else x
                        ),
                        return_dtype=pl.Float64,
                    )
                    .alias(col)
                )

        if clean_exprs:
            df = df.with_columns(clean_exprs)

        # Forward fill any remaining NaNs
        return df.fill_null(strategy="forward").fill_null(0.0)


def clean_features_polars(df: pl.DataFrame, features: List[str]) -> pl.DataFrame:
    """Clean features using Polars (memory efficient)."""
    logger.info("Cleaning features using Polars: handling infinity and NaN values...")

    # Build cleaning expressions for all features at once
    cleaning_exprs = []

    for col in features:
        if col in df.columns:
            # Replace inf/-inf with null, then handle outliers
            col_expr = (
                pl.col(col)
                .fill_nan(None)  # Convert NaN to null
                .map_elements(
                    lambda x: None if (x == float("inf") or x == float("-inf")) else x,
                    return_dtype=pl.Float64,
                )
            )

            # Calculate quantiles for outlier capping
            q99 = df[col].quantile(0.999)
            q01 = df[col].quantile(0.001)

            if (
                q99 is not None
                and q01 is not None
                and not (np.isnan(q99) or np.isnan(q01))
            ):
                # Clip extreme values
                col_expr = col_expr.clip(lower_bound=q01, upper_bound=q99)

            cleaning_exprs.append(col_expr.alias(col))

    # Apply all cleaning expressions at once
    if cleaning_exprs:
        df = df.with_columns(cleaning_exprs)

    # Handle remaining null values by forward fill, then zero fill
    df = df.fill_null(strategy="forward").fill_null(0.0)

    # Final validation - remove any rows that still have problematic values
    initial_rows = len(df)
    df = df.filter(
        ~pl.any_horizontal(
            [
                pl.col(col).is_null() | pl.col(col).is_infinite() | pl.col(col).is_nan()
                for col in features
                if col in df.columns
            ]
        )
    )
    final_rows = len(df)

    if initial_rows != final_rows:
        logger.info(f"Removed {initial_rows - final_rows} rows with problematic values")

    logger.info(
        f"✓ Feature cleaning completed using Polars. Final data shape: {df.shape}"
    )
    return df


def scale_features_polars(
    df: pl.DataFrame, features: List[str]
) -> tuple[pl.DataFrame, dict]:
    """Scale features using Polars MinMax scaling (memory efficient)."""
    logger.info("Scaling features using Polars MinMax scaling...")

    # Calculate min/max for each feature
    scaling_params = {}
    scale_exprs = []

    for col in features:
        if col in df.columns:
            col_min = df[col].min()
            col_max = df[col].max()
            scaling_params[col] = {"min": col_min, "max": col_max}

            # Scale to [0,1] range, handle edge case where min == max
            if col_max != col_min:
                scale_expr = ((pl.col(col) - col_min) / (col_max - col_min)).alias(col)
            else:
                scale_expr = pl.lit(0.0).alias(col)  # If constant, set to 0

            scale_exprs.append(scale_expr)

    # Apply scaling
    if scale_exprs:
        df = df.with_columns(scale_exprs)

    logger.info(f"✓ Scaled {len(features)} features using Polars MinMax scaling")
    return df, scaling_params


def apply_scaling_polars(
    df: pl.DataFrame, features: List[str], scaling_params: dict
) -> pl.DataFrame:
    """Apply pre-computed scaling parameters to features."""
    logger.info("Applying pre-computed scaling parameters...")

    scale_exprs = []
    for col in features:
        if col in df.columns and col in scaling_params:
            params = scaling_params[col]
            col_min = params["min"]
            col_max = params["max"]

            # Apply same scaling as training data
            if col_max != col_min:
                scale_expr = ((pl.col(col) - col_min) / (col_max - col_min)).alias(col)
            else:
                scale_expr = pl.lit(0.0).alias(col)

            scale_exprs.append(scale_expr)

    if scale_exprs:
        df = df.with_columns(scale_exprs)

    logger.info(f"✓ Applied scaling to {len(features)} features")
    return df
