#!/usr/bin/env python3

# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: MIT-0

"""
Sample data generator for the Order Flow Analysis pipeline.
This script generates synthetic financial market data for testing purposes.
"""

import pandas as pd
import numpy as np
import argparse
import boto3
from datetime import datetime
from pathlib import Path
import json
import logging

# Setup logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


class FinancialDataGenerator:
    """Generator for synthetic financial market data"""

    def __init__(self, seed: int = 42):
        """Initialize the generator with a random seed for reproducibility"""
        np.random.seed(seed)
        self.seed = seed

    def generate_tick_data(
        self,
        start_date: str,
        end_date: str,
        instrument_id: str = "72089",
        base_price: float = 100.0,
        volatility: float = 0.02,
        tick_frequency: str = "1S",
    ) -> pd.DataFrame:
        """
        Generate synthetic tick-by-tick market data

        Args:
            start_date: Start date in YYYY-MM-DD format
            end_date: End date in YYYY-MM-DD format
            instrument_id: Financial instrument identifier
            base_price: Base price for the synthetic instrument
            volatility: Price volatility (standard deviation)
            tick_frequency: Frequency of ticks (e.g., '1S' for 1 second)

        Returns:
            DataFrame with synthetic market data
        """
        logger.info(f"Generating synthetic data from {start_date} to {end_date}")

        # Create date range
        date_range = pd.date_range(start=start_date, end=end_date, freq=tick_frequency)

        n_ticks = len(date_range)
        logger.info(f"Generating {n_ticks:,} data points")

        # Generate price movements using geometric Brownian motion
        dt = 1 / (365 * 24 * 3600)  # Time step in years (for second-by-second data)
        price_changes = np.random.normal(0, volatility * np.sqrt(dt), n_ticks)
        prices = base_price * np.exp(np.cumsum(price_changes))

        # Generate bid-ask spread (typically 0.01% to 0.1% of price)
        spreads = np.random.uniform(0.0001, 0.001, n_ticks) * prices
        bid_prices = prices - spreads / 2
        ask_prices = prices + spreads / 2

        # Generate volumes using exponential distribution
        trade_quantities = np.random.exponential(100, n_ticks)
        bid_quantities = np.random.exponential(50, n_ticks)
        ask_quantities = np.random.exponential(50, n_ticks)

        # Add some market microstructure patterns
        # Higher volume during "market hours" (simulate UTC market hours)
        hour_of_day = date_range.hour
        volume_multiplier = np.where(
            (hour_of_day >= 9) & (hour_of_day <= 16),  # Market hours
            np.random.uniform(1.5, 3.0, n_ticks),  # Higher volume
            np.random.uniform(0.5, 1.0, n_ticks),  # Lower volume
        )

        trade_quantities *= volume_multiplier
        bid_quantities *= volume_multiplier
        ask_quantities *= volume_multiplier

        # Create the dataframe
        data = pd.DataFrame(
            {
                "timestamp": date_range,
                "instrument_id": instrument_id,
                "trade_price": prices,
                "trade_quantity": trade_quantities,
                "bid_price": bid_prices,
                "ask_price": ask_prices,
                "bid_quantity": bid_quantities,
                "ask_quantity": ask_quantities,
            }
        )

        # Add some derived features that are commonly used
        data["spread"] = data["ask_price"] - data["bid_price"]
        data["mid_price"] = (data["bid_price"] + data["ask_price"]) / 2
        data["price_impact"] = data["trade_price"] - data["mid_price"]

        # Add order flow imbalance
        data["order_imbalance"] = (data["bid_quantity"] - data["ask_quantity"]) / (
            data["bid_quantity"] + data["ask_quantity"]
        )

        logger.info(f"Generated data shape: {data.shape}")
        logger.info(
            f"Price range: ${data['trade_price'].min():.2f} - ${data['trade_price'].max():.2f}"
        )

        return data

    def add_market_events(
        self, data: pd.DataFrame, n_events: int = 5, event_impact: float = 0.05
    ) -> pd.DataFrame:
        """Add synthetic market events (news, announcements, etc.)"""

        logger.info(f"Adding {n_events} market events")

        # Randomly select times for events
        event_indices = np.random.choice(len(data), n_events, replace=False)
        event_indices.sort()

        data_copy = data.copy()

        for idx in event_indices:
            # Create a price jump
            direction = np.random.choice([-1, 1])
            jump_size = np.random.uniform(0.01, event_impact)

            # Apply the jump and let it decay over time
            decay_length = np.random.randint(10, 100)  # Event impact lasts 10-100 ticks
            end_idx = min(idx + decay_length, len(data))

            decay_factor = np.exp(-np.linspace(0, 3, end_idx - idx))
            price_adjustment = (
                direction
                * jump_size
                * data_copy.iloc[idx]["trade_price"]
                * decay_factor
            )

            data_copy.iloc[
                idx:end_idx, data_copy.columns.get_loc("trade_price")
            ] += price_adjustment
            data_copy.iloc[
                idx:end_idx, data_copy.columns.get_loc("bid_price")
            ] += price_adjustment
            data_copy.iloc[
                idx:end_idx, data_copy.columns.get_loc("ask_price")
            ] += price_adjustment
            data_copy.iloc[
                idx:end_idx, data_copy.columns.get_loc("mid_price")
            ] += price_adjustment

        return data_copy

    def save_to_parquet(self, data: pd.DataFrame, output_path: Path):
        """Save data to Parquet format"""
        output_path.parent.mkdir(parents=True, exist_ok=True)
        data.to_parquet(output_path, compression="snappy")
        logger.info(f"Data saved to {output_path}")

    def upload_to_s3(self, local_path: Path, s3_bucket: str, s3_key: str):
        """Upload data to S3"""
        try:
            s3_client = boto3.client("s3")
            s3_client.upload_file(str(local_path), s3_bucket, s3_key)
            logger.info(f"Data uploaded to s3://{s3_bucket}/{s3_key}")
        except Exception as e:
            logger.error(f"Failed to upload to S3: {e}")
            raise


def create_sample_metadata(data_info: dict) -> dict:
    """Create sample Iceberg table metadata"""
    return {
        "format-version": 2,
        "table-uuid": "sample-table-uuid",
        "location": data_info.get("s3_location", "s3://your-bucket/sample-data/"),
        "last-sequence-number": 1,
        "last-updated-ms": int(datetime.now().timestamp() * 1000),
        "last-column-id": 11,
        "current-schema-id": 0,
        "schemas": [
            {
                "type": "struct",
                "schema-id": 0,
                "fields": [
                    {
                        "id": 1,
                        "name": "timestamp",
                        "required": True,
                        "type": "timestamptz",
                    },
                    {
                        "id": 2,
                        "name": "instrument_id",
                        "required": True,
                        "type": "string",
                    },
                    {
                        "id": 3,
                        "name": "trade_price",
                        "required": True,
                        "type": "double",
                    },
                    {
                        "id": 4,
                        "name": "trade_quantity",
                        "required": True,
                        "type": "double",
                    },
                    {"id": 5, "name": "bid_price", "required": True, "type": "double"},
                    {"id": 6, "name": "ask_price", "required": True, "type": "double"},
                    {
                        "id": 7,
                        "name": "bid_quantity",
                        "required": True,
                        "type": "double",
                    },
                    {
                        "id": 8,
                        "name": "ask_quantity",
                        "required": True,
                        "type": "double",
                    },
                    {"id": 9, "name": "spread", "required": False, "type": "double"},
                    {
                        "id": 10,
                        "name": "mid_price",
                        "required": False,
                        "type": "double",
                    },
                    {
                        "id": 11,
                        "name": "order_imbalance",
                        "required": False,
                        "type": "double",
                    },
                ],
            }
        ],
        "default-spec-id": 0,
        "partition-specs": [
            {
                "spec-id": 0,
                "fields": [
                    {
                        "name": "instrument_id",
                        "transform": "identity",
                        "source-id": 2,
                        "field-id": 1000,
                    }
                ],
            }
        ],
    }


def main():
    """Main function"""
    parser = argparse.ArgumentParser(
        description="Generate sample financial data for testing"
    )
    parser.add_argument("--start-date", required=True, help="Start date (YYYY-MM-DD)")
    parser.add_argument("--end-date", required=True, help="End date (YYYY-MM-DD)")
    parser.add_argument("--instrument-id", default="72089", help="Instrument ID")
    parser.add_argument("--base-price", type=float, default=100.0, help="Base price")
    parser.add_argument(
        "--volatility", type=float, default=0.02, help="Price volatility"
    )
    parser.add_argument(
        "--tick-frequency", default="1S", help="Tick frequency (e.g., 1S, 5S, 1min)"
    )
    parser.add_argument(
        "--output-dir", default="./sample_data", help="Output directory"
    )
    parser.add_argument("--s3-bucket", help="S3 bucket to upload data (optional)")
    parser.add_argument("--s3-prefix", default="sample-data", help="S3 prefix")
    parser.add_argument("--add-events", action="store_true", help="Add market events")
    parser.add_argument("--seed", type=int, default=42, help="Random seed")

    args = parser.parse_args()

    # Create generator
    generator = FinancialDataGenerator(seed=args.seed)

    # Generate data
    data = generator.generate_tick_data(
        start_date=args.start_date,
        end_date=args.end_date,
        instrument_id=args.instrument_id,
        base_price=args.base_price,
        volatility=args.volatility,
        tick_frequency=args.tick_frequency,
    )

    # Add market events if requested
    if args.add_events:
        data = generator.add_market_events(data)

    # Save locally
    output_dir = Path(args.output_dir)
    output_file = (
        output_dir
        / f"sample_data_{args.instrument_id}_{args.start_date}_{args.end_date}.parquet"
    )
    generator.save_to_parquet(data, output_file)

    # Create metadata
    metadata = create_sample_metadata(
        {
            "instrument_id": args.instrument_id,
            "start_date": args.start_date,
            "end_date": args.end_date,
            "s3_location": (
                f"s3://{args.s3_bucket}/{args.s3_prefix}/" if args.s3_bucket else None
            ),
        }
    )

    metadata_file = output_dir / f"sample_metadata_{args.instrument_id}.json"
    with open(metadata_file, "w") as f:
        json.dump(metadata, f, indent=2)

    logger.info(f"Metadata saved to {metadata_file}")

    # Upload to S3 if requested
    if args.s3_bucket:
        s3_key = f"{args.s3_prefix}/data/{args.instrument_id}/{output_file.name}"
        generator.upload_to_s3(output_file, args.s3_bucket, s3_key)

        metadata_s3_key = (
            f"{args.s3_prefix}/metadata/{args.instrument_id}/metadata.json"
        )
        generator.upload_to_s3(metadata_file, args.s3_bucket, metadata_s3_key)

    # Print summary
    print("\n" + "=" * 60)
    print("Sample Data Generation Complete!")
    print("=" * 60)
    print(f"Data points: {len(data):,}")
    print(f"Time range: {data['timestamp'].min()} to {data['timestamp'].max()}")
    print(
        f"Price range: ${data['trade_price'].min():.2f} - ${data['trade_price'].max():.2f}"
    )
    print(f"Local file: {output_file}")
    if args.s3_bucket:
        print(f"S3 location: s3://{args.s3_bucket}/{s3_key}")
    print("\nNext steps:")
    print("1. Update your batch_config.json with the data location")
    print("2. Run the ML pipeline with this sample data")


if __name__ == "__main__":
    main()
