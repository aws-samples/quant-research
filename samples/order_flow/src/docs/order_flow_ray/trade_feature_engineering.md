# Trade Feature Engineering Documentation

## Overview

The `TradeFeatureEngineering` class processes normalized trade data and aggregates it into time-based bars with comprehensive features for quantitative analysis.

## Configuration

- **Bar Duration**: Configurable time window in milliseconds (e.g., 250ms, 1000ms, 5000ms)
- **Timestamp Column**: `TradeTimestampNanoseconds` (nanosecond precision)
- **Grouping Keys**: `bar_id`, `TradeDate`, `ExchangeTicker`, `Ticker`, `ISOExchangeCode`, `MIC`, `OPOL`, `ExecutionVenue`

## Feature Categories

### 1. Bar Metadata Features

| Feature | Description | Type |
|---------|-------------|------|
| `bar_id` | Bar identifier (epoch milliseconds) | Integer |
| `bar_id_dt` | Human-readable bar timestamp | Datetime |
| `bar_duration_ms` | Bar duration in milliseconds | Integer |

### 2. Timestamp Features

| Feature | Description | Calculation |
|---------|-------------|-------------|
| `bar_start_ns` | First trade timestamp in bar | `min(TradeTimestampNanoseconds)` |
| `bar_end_ns` | Last trade timestamp in bar | `max(TradeTimestampNanoseconds)` |
| `trade_count` | Number of trades in bar | `count(TradeTimestampNanoseconds)` |
| `trade_timestamp_min` | Minimum trade timestamp | `min(TradeTimestamp)` |
| `trade_timestamp_max` | Maximum trade timestamp | `max(TradeTimestamp)` |
| `publication_timestamp_min` | Minimum publication timestamp | `min(PublicationTimestamp)` |
| `publication_timestamp_max` | Maximum publication timestamp | `max(PublicationTimestamp)` |
| `local_trade_timestamp_min` | Minimum local trade timestamp | `min(LocalTradeTimestamp)` |
| `local_trade_timestamp_max` | Maximum local trade timestamp | `max(LocalTradeTimestamp)` |
| `local_publication_timestamp_min` | Minimum local publication timestamp | `min(LocalPublicationTimestamp)` |
| `local_publication_timestamp_max` | Maximum local publication timestamp | `max(LocalPublicationTimestamp)` |

### 3. Price Features (OHLC)

| Feature | Description | Calculation |
|---------|-------------|-------------|
| `price_open` | Opening price | `first(Price)` |
| `price_high` | Highest price | `max(Price)` |
| `price_low` | Lowest price | `min(Price)` |
| `price_close` | Closing price | `last(Price)` |
| `price_mean` | Average price | `mean(Price)` |

### 4. Volume Features

| Feature | Description | Calculation |
|---------|-------------|-------------|
| `volume` | Total volume traded | `sum(Size)` |
| `avg_trade_size` | Average trade size | `mean(Size)` |
| `max_trade_size` | Maximum trade size | `max(Size)` |

### 5. VWAP Features

| Feature | Description | Calculation |
|---------|-------------|-------------|
| `price_volume_sum` | Sum of price × volume | `sum(Price * Size)` |
| `vwap` | Volume Weighted Average Price | `sum(Price * Size) / sum(Size)` |

### 6. Order Flow Imbalance Features

#### Side Sign Calculation
```python
side_sign = when(AggressorSide == 1).then(1)     # Buy side
           .when(AggressorSide == 2).then(-1)    # Sell side  
           .otherwise(0)                         # Unassigned
```

| Feature | Description | Calculation |
|---------|-------------|-------------|
| `volume_imbalance` | Dollar volume imbalance | `sum(Size * Price * side_sign)` |
| `trade_imbalance` | Trade count imbalance | `sum(side_sign)` |
| `unassigned_volume` | Volume with no aggressor side | `sum(Size * Price) where AggressorSide == 0` |
| `unassigned_count` | Count of unassigned trades | `count() where AggressorSide == 0` |
| `total_volume` | Total dollar volume | `sum(Size * Price)` |
| `total_trades` | Total trade count | `count()` |

### 7. Ratio Features

| Feature | Description | Calculation |
|---------|-------------|-------------|
| `volume_imbalance_ratio` | Volume imbalance as % of total | `volume_imbalance / total_volume` |
| `trade_imbalance_ratio` | Trade imbalance as % of total | `trade_imbalance / total_trades` |
| `unassigned_volume_ratio` | Unassigned volume as % of total | `unassigned_volume / total_volume` |
| `unassigned_count_ratio` | Unassigned trades as % of total | `unassigned_count / total_trades` |

## Aggressor Side Mapping

| Value | Side | Description |
|-------|------|-------------|
| 1 | Buy | Buyer initiated trade |
| 2 | Sell | Seller initiated trade |
| 0 | Unassigned | No clear aggressor |

## Usage Example

```python
from feature_engineering.order_flow import TradeFeatureEngineering

# Initialize with 1-second bars
fe = TradeFeatureEngineering(bar_duration_ms=1000)

# Process trade data
result = fe.feature_computation(raw_trade_data)
aggregated_features = result.collect()
```

## Output Schema

The output contains 40 columns total:
- 8 grouping columns (bar_id, TradeDate, etc.)
- 32 feature columns as documented above

## Key Insights

1. **Order Flow Analysis**: Imbalance ratios indicate buying/selling pressure
2. **Liquidity Metrics**: Trade count and average size show market activity
3. **Price Discovery**: OHLC and VWAP provide price movement context
4. **Market Microstructure**: Timestamp features capture latency patterns
5. **Data Quality**: Unassigned ratios indicate classification accuracy