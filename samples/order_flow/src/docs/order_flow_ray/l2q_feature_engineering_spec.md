# L2Q Feature Engineering Specification

## Overview

The `L2QFeatureEngineering` class will process normalized Level 2 Quote data and aggregate it into time-based bars with comprehensive order book features for quantitative analysis.

## Configuration

- **Bar Duration**: Configurable time window in milliseconds (e.g., 250ms, 1000ms, 5000ms)
- **Timestamp Column**: `TimestampNanoseconds` (nanosecond precision)
- **Grouping Keys**: `bar_id`, `TradeDate`, `Ticker`, `ISOExchangeCode`, `MIC`, `ExchangeTicker`, `market_state_mode`

## Available L2Q Data Fields

### Core Fields
- `Ticker`, `ISOExchangeCode`, `TradeDate`, `TimestampNanoseconds`
- `MIC`, `ExchangeTicker`, `CurrencyCode`, `MarketState`

### Price/Quantity Data (10 levels each side)
- `BidPrice1-10`, `BidQuantity1-10`, `BidNumOrders1-10`
- `AskPrice1-10`, `AskQuantity1-10`, `AskNumOrders1-10`

### Metadata
- `BidLevelCount`, `AskLevelCount`, `L2EventNo`
- `EventTimestamp`, `LocalTimestamp`, `TZOffset`

## Proposed Feature Categories

### 1. Bar Metadata Features
| Feature | Description | Type |
|---------|-------------|------|
| `bar_id` | Bar identifier (epoch milliseconds) | Integer |
| `bar_id_dt` | Human-readable bar timestamp | Datetime |
| `bar_duration_ms` | Bar duration in milliseconds | Integer |
| `market_state_mode` | Most common market state in bar | String |

### 2. Quote Activity Features
| Feature | Description | Calculation |
|---------|-------------|-------------|
| `bar_start_dt` | First quote timestamp in bar | `min(bar_id_dt)` |
| `bar_end_dt` | Last quote timestamp in bar | `max(bar_id_dt)` |
| `quote_count` | Number of quotes in bar | `count(TimestampNanoseconds)` |
| `bid_update_count_l1` | Bid level 1 field changes | `sum(price_change + quantity_change + orders_change)` |
| `bid_update_count_l2` | Bid level 2 field changes | `sum(price_change + quantity_change + orders_change)` |
| `bid_update_count_l3` | Bid level 3 field changes | `sum(price_change + quantity_change + orders_change)` |
| `bid_update_count_l4` | Bid level 4 field changes | `sum(price_change + quantity_change + orders_change)` |
| `bid_update_count_l5` | Bid level 5 field changes | `sum(price_change + quantity_change + orders_change)` |
| `bid_update_count_l6` | Bid level 6 field changes | `sum(price_change + quantity_change + orders_change)` |
| `bid_update_count_l7` | Bid level 7 field changes | `sum(price_change + quantity_change + orders_change)` |
| `bid_update_count_l8` | Bid level 8 field changes | `sum(price_change + quantity_change + orders_change)` |
| `bid_update_count_l9` | Bid level 9 field changes | `sum(price_change + quantity_change + orders_change)` |
| `bid_update_count_l10` | Bid level 10 field changes | `sum(price_change + quantity_change + orders_change)` |
| `ask_update_count_l1` | Ask level 1 field changes | `sum(price_change + quantity_change + orders_change)` |
| `ask_update_count_l2` | Ask level 2 field changes | `sum(price_change + quantity_change + orders_change)` |
| `ask_update_count_l3` | Ask level 3 field changes | `sum(price_change + quantity_change + orders_change)` |
| `ask_update_count_l4` | Ask level 4 field changes | `sum(price_change + quantity_change + orders_change)` |
| `ask_update_count_l5` | Ask level 5 field changes | `sum(price_change + quantity_change + orders_change)` |
| `ask_update_count_l6` | Ask level 6 field changes | `sum(price_change + quantity_change + orders_change)` |
| `ask_update_count_l7` | Ask level 7 field changes | `sum(price_change + quantity_change + orders_change)` |
| `ask_update_count_l8` | Ask level 8 field changes | `sum(price_change + quantity_change + orders_change)` |
| `ask_update_count_l9` | Ask level 9 field changes | `sum(price_change + quantity_change + orders_change)` |
| `ask_update_count_l10` | Ask level 10 field changes | `sum(price_change + quantity_change + orders_change)` |

### 3. Spread Features
| Feature | Description | Calculation |
|---------|-------------|-------------|
| `volume_weighted_mid_l1` | Volume-weighted mid price level 1 | `mid((BidPrice1 * BidQuantity1 + AskPrice1 * AskQuantity1) / (BidQuantity1 + AskQuantity1))` |
| `volume_weighted_mid_l2` | Volume-weighted mid price level 2 | `mid((BidPrice2 * BidQuantity2 + AskPrice2 * AskQuantity2) / (BidQuantity2 + AskQuantity2))` |
| `volume_weighted_mid_l3` | Volume-weighted mid price level 3 | `mid((BidPrice3 * BidQuantity3 + AskPrice3 * AskQuantity3) / (BidQuantity3 + AskQuantity3))` |
| `volume_weighted_mid_l4` | Volume-weighted mid price level 4 | `mid((BidPrice4 * BidQuantity4 + AskPrice4 * AskQuantity4) / (BidQuantity4 + AskQuantity4))` |
| `volume_weighted_mid_l5` | Volume-weighted mid price level 5 | `mid((BidPrice5 * BidQuantity5 + AskPrice5 * AskQuantity5) / (BidQuantity5 + AskQuantity5))` |
| `volume_weighted_mid_l6` | Volume-weighted mid price level 6 | `mid((BidPrice6 * BidQuantity6 + AskPrice6 * AskQuantity6) / (BidQuantity6 + AskQuantity6))` |
| `volume_weighted_mid_l7` | Volume-weighted mid price level 7 | `mid((BidPrice7 * BidQuantity7 + AskPrice7 * AskQuantity7) / (BidQuantity7 + AskQuantity7))` |
| `volume_weighted_mid_l8` | Volume-weighted mid price level 8 | `mid((BidPrice8 * BidQuantity8 + AskPrice8 * AskQuantity8) / (BidQuantity8 + AskQuantity8))` |
| `volume_weighted_mid_l9` | Volume-weighted mid price level 9 | `mid((BidPrice9 * BidQuantity9 + AskPrice9 * AskQuantity9) / (BidQuantity9 + AskQuantity9))` |
| `volume_weighted_mid_l10` | Volume-weighted mid price level 10 | `mid((BidPrice10 * BidQuantity10 + AskPrice10 * AskQuantity10) / (BidQuantity10 + AskQuantity10))` |
| `spread_ratio_l1` | Spread ratio level 1 | `mid((AskPrice1 - BidPrice1) / ((BidPrice1 + AskPrice1) / 2))` |
| `spread_ratio_l2` | Spread ratio level 2 | `mid((AskPrice2 - BidPrice2) / ((BidPrice2 + AskPrice2) / 2))` |
| `spread_ratio_l3` | Spread ratio level 3 | `mid((AskPrice3 - BidPrice3) / ((BidPrice3 + AskPrice3) / 2))` |
| `spread_ratio_l4` | Spread ratio level 4 | `mid((AskPrice4 - BidPrice4) / ((BidPrice4 + AskPrice4) / 2))` |
| `spread_ratio_l5` | Spread ratio level 5 | `mid((AskPrice5 - BidPrice5) / ((BidPrice5 + AskPrice5) / 2))` |
| `spread_ratio_l6` | Spread ratio level 6 | `mid((AskPrice6 - BidPrice6) / ((BidPrice6 + AskPrice6) / 2))` |
| `spread_ratio_l7` | Spread ratio level 7 | `mid((AskPrice7 - BidPrice7) / ((BidPrice7 + AskPrice7) / 2))` |
| `spread_ratio_l8` | Spread ratio level 8 | `mid((AskPrice8 - BidPrice8) / ((BidPrice8 + AskPrice8) / 2))` |
| `spread_ratio_l9` | Spread ratio level 9 | `mid((AskPrice9 - BidPrice9) / ((BidPrice9 + AskPrice9) / 2))` |
| `spread_ratio_l10` | Spread ratio level 10 | `mid((AskPrice10 - BidPrice10) / ((BidPrice10 + AskPrice10) / 2))` |

### 4. Quantity Features
| Feature | Description | Calculation |
|---------|-------------|-------------|
| `quantity_imbalance_l1` | Quantity imbalance level 1 | `mid((AskQuantity1 - BidQuantity1) / (AskQuantity1 + BidQuantity1))` |
| `quantity_imbalance_l2` | Quantity imbalance level 2 | `mid((AskQuantity2 - BidQuantity2) / (AskQuantity2 + BidQuantity2))` |
| `quantity_imbalance_l3` | Quantity imbalance level 3 | `mid((AskQuantity3 - BidQuantity3) / (AskQuantity3 + BidQuantity3))` |
| `quantity_imbalance_l4` | Quantity imbalance level 4 | `mid((AskQuantity4 - BidQuantity4) / (AskQuantity4 + BidQuantity4))` |
| `quantity_imbalance_l5` | Quantity imbalance level 5 | `mid((AskQuantity5 - BidQuantity5) / (AskQuantity5 + BidQuantity5))` |
| `quantity_imbalance_l6` | Quantity imbalance level 6 | `mid((AskQuantity6 - BidQuantity6) / (AskQuantity6 + BidQuantity6))` |
| `quantity_imbalance_l7` | Quantity imbalance level 7 | `mid((AskQuantity7 - BidQuantity7) / (AskQuantity7 + BidQuantity7))` |
| `quantity_imbalance_l8` | Quantity imbalance level 8 | `mid((AskQuantity8 - BidQuantity8) / (AskQuantity8 + BidQuantity8))` |
| `quantity_imbalance_l9` | Quantity imbalance level 9 | `mid((AskQuantity9 - BidQuantity9) / (AskQuantity9 + BidQuantity9))` |
| `quantity_imbalance_l10` | Quantity imbalance level 10 | `mid((AskQuantity10 - BidQuantity10) / (AskQuantity10 + BidQuantity10))` |

### 5. Volume Features
| Feature | Description | Calculation |
|---------|-------------|-------------|
| `volume_imbalance_l1` | Volume imbalance level 1 | `mid((AskPrice1 * AskQuantity1 - BidPrice1 * BidQuantity1) / (AskPrice1 * AskQuantity1 + BidPrice1 * BidQuantity1))` |
| `volume_imbalance_l2` | Volume imbalance level 2 | `mid((AskPrice2 * AskQuantity2 - BidPrice2 * BidQuantity2) / (AskPrice2 * AskQuantity2 + BidPrice2 * BidQuantity2))` |
| `volume_imbalance_l3` | Volume imbalance level 3 | `mid((AskPrice3 * AskQuantity3 - BidPrice3 * BidQuantity3) / (AskPrice3 * AskQuantity3 + BidPrice3 * BidQuantity3))` |
| `volume_imbalance_l4` | Volume imbalance level 4 | `mid((AskPrice4 * AskQuantity4 - BidPrice4 * BidQuantity4) / (AskPrice4 * AskQuantity4 + BidPrice4 * BidQuantity4))` |
| `volume_imbalance_l5` | Volume imbalance level 5 | `mid((AskPrice5 * AskQuantity5 - BidPrice5 * BidQuantity5) / (AskPrice5 * AskQuantity5 + BidPrice5 * BidQuantity5))` |
| `volume_imbalance_l6` | Volume imbalance level 6 | `mid((AskPrice6 * AskQuantity6 - BidPrice6 * BidQuantity6) / (AskPrice6 * AskQuantity6 + BidPrice6 * BidQuantity6))` |
| `volume_imbalance_l7` | Volume imbalance level 7 | `mid((AskPrice7 * AskQuantity7 - BidPrice7 * BidQuantity7) / (AskPrice7 * AskQuantity7 + BidPrice7 * BidQuantity7))` |
| `volume_imbalance_l8` | Volume imbalance level 8 | `mid((AskPrice8 * AskQuantity8 - BidPrice8 * BidQuantity8) / (AskPrice8 * AskQuantity8 + BidPrice8 * BidQuantity8))` |
| `volume_imbalance_l9` | Volume imbalance level 9 | `mid((AskPrice9 * AskQuantity9 - BidPrice9 * BidQuantity9) / (AskPrice9 * AskQuantity9 + BidPrice9 * BidQuantity9))` |
| `volume_imbalance_l10` | Volume imbalance level 10 | `mid((AskPrice10 * AskQuantity10 - BidPrice10 * BidQuantity10) / (AskPrice10 * AskQuantity10 + BidPrice10 * BidQuantity10))` |

### 6. Volatility Features
| Feature | Description | Calculation |
|---------|-------------|-------------|
| `bid_price_volatility_l1` | Bid price volatility level 1 | `stdev(BidPrice1) / mid(BidPrice1)` |
| `bid_price_volatility_l2` | Bid price volatility level 2 | `stdev(BidPrice2) / mid(BidPrice2)` |
| `bid_price_volatility_l3` | Bid price volatility level 3 | `stdev(BidPrice3) / mid(BidPrice3)` |
| `bid_price_volatility_l4` | Bid price volatility level 4 | `stdev(BidPrice4) / mid(BidPrice4)` |
| `bid_price_volatility_l5` | Bid price volatility level 5 | `stdev(BidPrice5) / mid(BidPrice5)` |
| `bid_price_volatility_l6` | Bid price volatility level 6 | `stdev(BidPrice6) / mid(BidPrice6)` |
| `bid_price_volatility_l7` | Bid price volatility level 7 | `stdev(BidPrice7) / mid(BidPrice7)` |
| `bid_price_volatility_l8` | Bid price volatility level 8 | `stdev(BidPrice8) / mid(BidPrice8)` |
| `bid_price_volatility_l9` | Bid price volatility level 9 | `stdev(BidPrice9) / mid(BidPrice9)` |
| `bid_price_volatility_l10` | Bid price volatility level 10 | `stdev(BidPrice10) / mid(BidPrice10)` |
| `ask_price_volatility_l1` | Ask price volatility level 1 | `stdev(AskPrice1) / mid(AskPrice1)` |
| `ask_price_volatility_l2` | Ask price volatility level 2 | `stdev(AskPrice2) / mid(AskPrice2)` |
| `ask_price_volatility_l3` | Ask price volatility level 3 | `stdev(AskPrice3) / mid(AskPrice3)` |
| `ask_price_volatility_l4` | Ask price volatility level 4 | `stdev(AskPrice4) / mid(AskPrice4)` |
| `ask_price_volatility_l5` | Ask price volatility level 5 | `stdev(AskPrice5) / mid(AskPrice5)` |
| `ask_price_volatility_l6` | Ask price volatility level 6 | `stdev(AskPrice6) / mid(AskPrice6)` |
| `ask_price_volatility_l7` | Ask price volatility level 7 | `stdev(AskPrice7) / mid(AskPrice7)` |
| `ask_price_volatility_l8` | Ask price volatility level 8 | `stdev(AskPrice8) / mid(AskPrice8)` |
| `ask_price_volatility_l9` | Ask price volatility level 9 | `stdev(AskPrice9) / mid(AskPrice9)` |
| `ask_price_volatility_l10` | Ask price volatility level 10 | `stdev(AskPrice10) / mid(AskPrice10)` |
| `mid_price_volatility_l1` | Mid price volatility level 1 | `stdev((BidPrice1 + AskPrice1) / 2) / mid((BidPrice1 + AskPrice1) / 2)` |
| `mid_price_volatility_l2` | Mid price volatility level 2 | `stdev((BidPrice2 + AskPrice2) / 2) / mid((BidPrice2 + AskPrice2) / 2)` |
| `mid_price_volatility_l3` | Mid price volatility level 3 | `stdev((BidPrice3 + AskPrice3) / 2) / mid((BidPrice3 + AskPrice3) / 2)` |
| `mid_price_volatility_l4` | Mid price volatility level 4 | `stdev((BidPrice4 + AskPrice4) / 2) / mid((BidPrice4 + AskPrice4) / 2)` |
| `mid_price_volatility_l5` | Mid price volatility level 5 | `stdev((BidPrice5 + AskPrice5) / 2) / mid((BidPrice5 + AskPrice5) / 2)` |
| `mid_price_volatility_l6` | Mid price volatility level 6 | `stdev((BidPrice6 + AskPrice6) / 2) / mid((BidPrice6 + AskPrice6) / 2)` |
| `mid_price_volatility_l7` | Mid price volatility level 7 | `stdev((BidPrice7 + AskPrice7) / 2) / mid((BidPrice7 + AskPrice7) / 2)` |
| `mid_price_volatility_l8` | Mid price volatility level 8 | `stdev((BidPrice8 + AskPrice8) / 2) / mid((BidPrice8 + AskPrice8) / 2)` |
| `mid_price_volatility_l9` | Mid price volatility level 9 | `stdev((BidPrice9 + AskPrice9) / 2) / mid((BidPrice9 + AskPrice9) / 2)` |
| `mid_price_volatility_l10` | Mid price volatility level 10 | `stdev((BidPrice10 + AskPrice10) / 2) / mid((BidPrice10 + AskPrice10) / 2)` |
| `bid_quantity_volatility_l1` | Bid quantity volatility level 1 | `stdev(BidQuantity1) / mid(BidQuantity1)` |
| `bid_quantity_volatility_l2` | Bid quantity volatility level 2 | `stdev(BidQuantity2) / mid(BidQuantity2)` |
| `bid_quantity_volatility_l3` | Bid quantity volatility level 3 | `stdev(BidQuantity3) / mid(BidQuantity3)` |
| `bid_quantity_volatility_l4` | Bid quantity volatility level 4 | `stdev(BidQuantity4) / mid(BidQuantity4)` |
| `bid_quantity_volatility_l5` | Bid quantity volatility level 5 | `stdev(BidQuantity5) / mid(BidQuantity5)` |
| `bid_quantity_volatility_l6` | Bid quantity volatility level 6 | `stdev(BidQuantity6) / mid(BidQuantity6)` |
| `bid_quantity_volatility_l7` | Bid quantity volatility level 7 | `stdev(BidQuantity7) / mid(BidQuantity7)` |
| `bid_quantity_volatility_l8` | Bid quantity volatility level 8 | `stdev(BidQuantity8) / mid(BidQuantity8)` |
| `bid_quantity_volatility_l9` | Bid quantity volatility level 9 | `stdev(BidQuantity9) / mid(BidQuantity9)` |
| `bid_quantity_volatility_l10` | Bid quantity volatility level 10 | `stdev(BidQuantity10) / mid(BidQuantity10)` |
| `ask_quantity_volatility_l1` | Ask quantity volatility level 1 | `stdev(AskQuantity1) / mid(AskQuantity1)` |
| `ask_quantity_volatility_l2` | Ask quantity volatility level 2 | `stdev(AskQuantity2) / mid(AskQuantity2)` |
| `ask_quantity_volatility_l3` | Ask quantity volatility level 3 | `stdev(AskQuantity3) / mid(AskQuantity3)` |
| `ask_quantity_volatility_l4` | Ask quantity volatility level 4 | `stdev(AskQuantity4) / mid(AskQuantity4)` |
| `ask_quantity_volatility_l5` | Ask quantity volatility level 5 | `stdev(AskQuantity5) / mid(AskQuantity5)` |
| `ask_quantity_volatility_l6` | Ask quantity volatility level 6 | `stdev(AskQuantity6) / mid(AskQuantity6)` |
| `ask_quantity_volatility_l7` | Ask quantity volatility level 7 | `stdev(AskQuantity7) / mid(AskQuantity7)` |
| `ask_quantity_volatility_l8` | Ask quantity volatility level 8 | `stdev(AskQuantity8) / mid(AskQuantity8)` |
| `ask_quantity_volatility_l9` | Ask quantity volatility level 9 | `stdev(AskQuantity9) / mid(AskQuantity9)` |
| `ask_quantity_volatility_l10` | Ask quantity volatility level 10 | `stdev(AskQuantity10) / mid(AskQuantity10)` |
| `bid_volume_volatility_l1` | Bid volume volatility level 1 | `stdev(BidPrice1 * BidQuantity1) / mid(BidPrice1 * BidQuantity1)` |
| `bid_volume_volatility_l2` | Bid volume volatility level 2 | `stdev(BidPrice2 * BidQuantity2) / mid(BidPrice2 * BidQuantity2)` |
| `bid_volume_volatility_l3` | Bid volume volatility level 3 | `stdev(BidPrice3 * BidQuantity3) / mid(BidPrice3 * BidQuantity3)` |
| `bid_volume_volatility_l4` | Bid volume volatility level 4 | `stdev(BidPrice4 * BidQuantity4) / mid(BidPrice4 * BidQuantity4)` |
| `bid_volume_volatility_l5` | Bid volume volatility level 5 | `stdev(BidPrice5 * BidQuantity5) / mid(BidPrice5 * BidQuantity5)` |
| `bid_volume_volatility_l6` | Bid volume volatility level 6 | `stdev(BidPrice6 * BidQuantity6) / mid(BidPrice6 * BidQuantity6)` |
| `bid_volume_volatility_l7` | Bid volume volatility level 7 | `stdev(BidPrice7 * BidQuantity7) / mid(BidPrice7 * BidQuantity7)` |
| `bid_volume_volatility_l8` | Bid volume volatility level 8 | `stdev(BidPrice8 * BidQuantity8) / mid(BidPrice8 * BidQuantity8)` |
| `bid_volume_volatility_l9` | Bid volume volatility level 9 | `stdev(BidPrice9 * BidQuantity9) / mid(BidPrice9 * BidQuantity9)` |
| `bid_volume_volatility_l10` | Bid volume volatility level 10 | `stdev(BidPrice10 * BidQuantity10) / mid(BidPrice10 * BidQuantity10)` |
| `ask_volume_volatility_l1` | Ask volume volatility level 1 | `stdev(AskPrice1 * AskQuantity1) / mid(AskPrice1 * AskQuantity1)` |
| `ask_volume_volatility_l2` | Ask volume volatility level 2 | `stdev(AskPrice2 * AskQuantity2) / mid(AskPrice2 * AskQuantity2)` |
| `ask_volume_volatility_l3` | Ask volume volatility level 3 | `stdev(AskPrice3 * AskQuantity3) / mid(AskPrice3 * AskQuantity3)` |
| `ask_volume_volatility_l4` | Ask volume volatility level 4 | `stdev(AskPrice4 * AskQuantity4) / mid(AskPrice4 * AskQuantity4)` |
| `ask_volume_volatility_l5` | Ask volume volatility level 5 | `stdev(AskPrice5 * AskQuantity5) / mid(AskPrice5 * AskQuantity5)` |
| `ask_volume_volatility_l6` | Ask volume volatility level 6 | `stdev(AskPrice6 * AskQuantity6) / mid(AskPrice6 * AskQuantity6)` |
| `ask_volume_volatility_l7` | Ask volume volatility level 7 | `stdev(AskPrice7 * AskQuantity7) / mid(AskPrice7 * AskQuantity7)` |
| `ask_volume_volatility_l8` | Ask volume volatility level 8 | `stdev(AskPrice8 * AskQuantity8) / mid(AskPrice8 * AskQuantity8)` |
| `ask_volume_volatility_l9` | Ask volume volatility level 9 | `stdev(AskPrice9 * AskQuantity9) / mid(AskPrice9 * AskQuantity9)` |
| `ask_volume_volatility_l10` | Ask volume volatility level 10 | `stdev(AskPrice10 * AskQuantity10) / mid(AskPrice10 * AskQuantity10)` |

### 7. Trend Features
| Feature | Description | Calculation |
|---------|-------------|-------------|
| `bid_price_trend_l1` | Bid price trend level 1 | `slope(BidPrice1 ORDER BY TimestampNanoseconds)` |
| `bid_price_trend_l2` | Bid price trend level 2 | `slope(BidPrice2 ORDER BY TimestampNanoseconds)` |
| `bid_price_trend_l3` | Bid price trend level 3 | `slope(BidPrice3 ORDER BY TimestampNanoseconds)` |
| `bid_price_trend_l4` | Bid price trend level 4 | `slope(BidPrice4 ORDER BY TimestampNanoseconds)` |
| `bid_price_trend_l5` | Bid price trend level 5 | `slope(BidPrice5 ORDER BY TimestampNanoseconds)` |
| `bid_price_trend_l6` | Bid price trend level 6 | `slope(BidPrice6 ORDER BY TimestampNanoseconds)` |
| `bid_price_trend_l7` | Bid price trend level 7 | `slope(BidPrice7 ORDER BY TimestampNanoseconds)` |
| `bid_price_trend_l8` | Bid price trend level 8 | `slope(BidPrice8 ORDER BY TimestampNanoseconds)` |
| `bid_price_trend_l9` | Bid price trend level 9 | `slope(BidPrice9 ORDER BY TimestampNanoseconds)` |
| `bid_price_trend_l10` | Bid price trend level 10 | `slope(BidPrice10 ORDER BY TimestampNanoseconds)` |
| `ask_price_trend_l1` | Ask price trend level 1 | `slope(AskPrice1 ORDER BY TimestampNanoseconds)` |
| `ask_price_trend_l2` | Ask price trend level 2 | `slope(AskPrice2 ORDER BY TimestampNanoseconds)` |
| `ask_price_trend_l3` | Ask price trend level 3 | `slope(AskPrice3 ORDER BY TimestampNanoseconds)` |
| `ask_price_trend_l4` | Ask price trend level 4 | `slope(AskPrice4 ORDER BY TimestampNanoseconds)` |
| `ask_price_trend_l5` | Ask price trend level 5 | `slope(AskPrice5 ORDER BY TimestampNanoseconds)` |
| `ask_price_trend_l6` | Ask price trend level 6 | `slope(AskPrice6 ORDER BY TimestampNanoseconds)` |
| `ask_price_trend_l7` | Ask price trend level 7 | `slope(AskPrice7 ORDER BY TimestampNanoseconds)` |
| `ask_price_trend_l8` | Ask price trend level 8 | `slope(AskPrice8 ORDER BY TimestampNanoseconds)` |
| `ask_price_trend_l9` | Ask price trend level 9 | `slope(AskPrice9 ORDER BY TimestampNanoseconds)` |
| `ask_price_trend_l10` | Ask price trend level 10 | `slope(AskPrice10 ORDER BY TimestampNanoseconds)` |
| `mid_price_trend_l1` | Mid price trend level 1 | `slope((BidPrice1 + AskPrice1) / 2 ORDER BY TimestampNanoseconds)` |
| `mid_price_trend_l2` | Mid price trend level 2 | `slope((BidPrice2 + AskPrice2) / 2 ORDER BY TimestampNanoseconds)` |
| `mid_price_trend_l3` | Mid price trend level 3 | `slope((BidPrice3 + AskPrice3) / 2 ORDER BY TimestampNanoseconds)` |
| `mid_price_trend_l4` | Mid price trend level 4 | `slope((BidPrice4 + AskPrice4) / 2 ORDER BY TimestampNanoseconds)` |
| `mid_price_trend_l5` | Mid price trend level 5 | `slope((BidPrice5 + AskPrice5) / 2 ORDER BY TimestampNanoseconds)` |
| `mid_price_trend_l6` | Mid price trend level 6 | `slope((BidPrice6 + AskPrice6) / 2 ORDER BY TimestampNanoseconds)` |
| `mid_price_trend_l7` | Mid price trend level 7 | `slope((BidPrice7 + AskPrice7) / 2 ORDER BY TimestampNanoseconds)` |
| `mid_price_trend_l8` | Mid price trend level 8 | `slope((BidPrice8 + AskPrice8) / 2 ORDER BY TimestampNanoseconds)` |
| `mid_price_trend_l9` | Mid price trend level 9 | `slope((BidPrice9 + AskPrice9) / 2 ORDER BY TimestampNanoseconds)` |
| `mid_price_trend_l10` | Mid price trend level 10 | `slope((BidPrice10 + AskPrice10) / 2 ORDER BY TimestampNanoseconds)` |
| `bid_quantity_trend_l1` | Bid quantity trend level 1 | `slope(BidQuantity1 ORDER BY TimestampNanoseconds)` |
| `bid_quantity_trend_l2` | Bid quantity trend level 2 | `slope(BidQuantity2 ORDER BY TimestampNanoseconds)` |
| `bid_quantity_trend_l3` | Bid quantity trend level 3 | `slope(BidQuantity3 ORDER BY TimestampNanoseconds)` |
| `bid_quantity_trend_l4` | Bid quantity trend level 4 | `slope(BidQuantity4 ORDER BY TimestampNanoseconds)` |
| `bid_quantity_trend_l5` | Bid quantity trend level 5 | `slope(BidQuantity5 ORDER BY TimestampNanoseconds)` |
| `bid_quantity_trend_l6` | Bid quantity trend level 6 | `slope(BidQuantity6 ORDER BY TimestampNanoseconds)` |
| `bid_quantity_trend_l7` | Bid quantity trend level 7 | `slope(BidQuantity7 ORDER BY TimestampNanoseconds)` |
| `bid_quantity_trend_l8` | Bid quantity trend level 8 | `slope(BidQuantity8 ORDER BY TimestampNanoseconds)` |
| `bid_quantity_trend_l9` | Bid quantity trend level 9 | `slope(BidQuantity9 ORDER BY TimestampNanoseconds)` |
| `bid_quantity_trend_l10` | Bid quantity trend level 10 | `slope(BidQuantity10 ORDER BY TimestampNanoseconds)` |
| `ask_quantity_trend_l1` | Ask quantity trend level 1 | `slope(AskQuantity1 ORDER BY TimestampNanoseconds)` |
| `ask_quantity_trend_l2` | Ask quantity trend level 2 | `slope(AskQuantity2 ORDER BY TimestampNanoseconds)` |
| `ask_quantity_trend_l3` | Ask quantity trend level 3 | `slope(AskQuantity3 ORDER BY TimestampNanoseconds)` |
| `ask_quantity_trend_l4` | Ask quantity trend level 4 | `slope(AskQuantity4 ORDER BY TimestampNanoseconds)` |
| `ask_quantity_trend_l5` | Ask quantity trend level 5 | `slope(AskQuantity5 ORDER BY TimestampNanoseconds)` |
| `ask_quantity_trend_l6` | Ask quantity trend level 6 | `slope(AskQuantity6 ORDER BY TimestampNanoseconds)` |
| `ask_quantity_trend_l7` | Ask quantity trend level 7 | `slope(AskQuantity7 ORDER BY TimestampNanoseconds)` |
| `ask_quantity_trend_l8` | Ask quantity trend level 8 | `slope(AskQuantity8 ORDER BY TimestampNanoseconds)` |
| `ask_quantity_trend_l9` | Ask quantity trend level 9 | `slope(AskQuantity9 ORDER BY TimestampNanoseconds)` |
| `ask_quantity_trend_l10` | Ask quantity trend level 10 | `slope(AskQuantity10 ORDER BY TimestampNanoseconds)` |
| `bid_volume_trend_l1` | Bid volume trend level 1 | `slope(BidPrice1 * BidQuantity1 ORDER BY TimestampNanoseconds)` |
| `bid_volume_trend_l2` | Bid volume trend level 2 | `slope(BidPrice2 * BidQuantity2 ORDER BY TimestampNanoseconds)` |
| `bid_volume_trend_l3` | Bid volume trend level 3 | `slope(BidPrice3 * BidQuantity3 ORDER BY TimestampNanoseconds)` |
| `bid_volume_trend_l4` | Bid volume trend level 4 | `slope(BidPrice4 * BidQuantity4 ORDER BY TimestampNanoseconds)` |
| `bid_volume_trend_l5` | Bid volume trend level 5 | `slope(BidPrice5 * BidQuantity5 ORDER BY TimestampNanoseconds)` |
| `bid_volume_trend_l6` | Bid volume trend level 6 | `slope(BidPrice6 * BidQuantity6 ORDER BY TimestampNanoseconds)` |
| `bid_volume_trend_l7` | Bid volume trend level 7 | `slope(BidPrice7 * BidQuantity7 ORDER BY TimestampNanoseconds)` |
| `bid_volume_trend_l8` | Bid volume trend level 8 | `slope(BidPrice8 * BidQuantity8 ORDER BY TimestampNanoseconds)` |
| `bid_volume_trend_l9` | Bid volume trend level 9 | `slope(BidPrice9 * BidQuantity9 ORDER BY TimestampNanoseconds)` |
| `bid_volume_trend_l10` | Bid volume trend level 10 | `slope(BidPrice10 * BidQuantity10 ORDER BY TimestampNanoseconds)` |
| `ask_volume_trend_l1` | Ask volume trend level 1 | `slope(AskPrice1 * AskQuantity1 ORDER BY TimestampNanoseconds)` |
| `ask_volume_trend_l2` | Ask volume trend level 2 | `slope(AskPrice2 * AskQuantity2 ORDER BY TimestampNanoseconds)` |
| `ask_volume_trend_l3` | Ask volume trend level 3 | `slope(AskPrice3 * AskQuantity3 ORDER BY TimestampNanoseconds)` |
| `ask_volume_trend_l4` | Ask volume trend level 4 | `slope(AskPrice4 * AskQuantity4 ORDER BY TimestampNanoseconds)` |
| `ask_volume_trend_l5` | Ask volume trend level 5 | `slope(AskPrice5 * AskQuantity5 ORDER BY TimestampNanoseconds)` |
| `ask_volume_trend_l6` | Ask volume trend level 6 | `slope(AskPrice6 * AskQuantity6 ORDER BY TimestampNanoseconds)` |
| `ask_volume_trend_l7` | Ask volume trend level 7 | `slope(AskPrice7 * AskQuantity7 ORDER BY TimestampNanoseconds)` |
| `ask_volume_trend_l8` | Ask volume trend level 8 | `slope(AskPrice8 * AskQuantity8 ORDER BY TimestampNanoseconds)` |
| `ask_volume_trend_l9` | Ask volume trend level 9 | `slope(AskPrice9 * AskQuantity9 ORDER BY TimestampNanoseconds)` |
| `ask_volume_trend_l10` | Ask volume trend level 10 | `slope(AskPrice10 * AskQuantity10 ORDER BY TimestampNanoseconds)` |

### 8. Trend Vol Features
| Feature | Description | Calculation |
|---------|-------------|-------------|
| `bid_price_trend_vol_l1` | Bid price trend volatility level 1 | `mse(BidPrice1 ORDER BY TimestampNanoseconds) / mid(BidPrice1)` |
| `bid_price_trend_vol_l2` | Bid price trend volatility level 2 | `mse(BidPrice2 ORDER BY TimestampNanoseconds) / mid(BidPrice2)` |
| `bid_price_trend_vol_l3` | Bid price trend volatility level 3 | `mse(BidPrice3 ORDER BY TimestampNanoseconds) / mid(BidPrice3)` |
| `bid_price_trend_vol_l4` | Bid price trend volatility level 4 | `mse(BidPrice4 ORDER BY TimestampNanoseconds) / mid(BidPrice4)` |
| `bid_price_trend_vol_l5` | Bid price trend volatility level 5 | `mse(BidPrice5 ORDER BY TimestampNanoseconds) / mid(BidPrice5)` |
| `bid_price_trend_vol_l6` | Bid price trend volatility level 6 | `mse(BidPrice6 ORDER BY TimestampNanoseconds) / mid(BidPrice6)` |
| `bid_price_trend_vol_l7` | Bid price trend volatility level 7 | `mse(BidPrice7 ORDER BY TimestampNanoseconds) / mid(BidPrice7)` |
| `bid_price_trend_vol_l8` | Bid price trend volatility level 8 | `mse(BidPrice8 ORDER BY TimestampNanoseconds) / mid(BidPrice8)` |
| `bid_price_trend_vol_l9` | Bid price trend volatility level 9 | `mse(BidPrice9 ORDER BY TimestampNanoseconds) / mid(BidPrice9)` |
| `bid_price_trend_vol_l10` | Bid price trend volatility level 10 | `mse(BidPrice10 ORDER BY TimestampNanoseconds) / mid(BidPrice10)` |
| `ask_price_trend_vol_l1` | Ask price trend volatility level 1 | `mse(AskPrice1 ORDER BY TimestampNanoseconds) / mid(AskPrice1)` |
| `ask_price_trend_vol_l2` | Ask price trend volatility level 2 | `mse(AskPrice2 ORDER BY TimestampNanoseconds) / mid(AskPrice2)` |
| `ask_price_trend_vol_l3` | Ask price trend volatility level 3 | `mse(AskPrice3 ORDER BY TimestampNanoseconds) / mid(AskPrice3)` |
| `ask_price_trend_vol_l4` | Ask price trend volatility level 4 | `mse(AskPrice4 ORDER BY TimestampNanoseconds) / mid(AskPrice4)` |
| `ask_price_trend_vol_l5` | Ask price trend volatility level 5 | `mse(AskPrice5 ORDER BY TimestampNanoseconds) / mid(AskPrice5)` |
| `ask_price_trend_vol_l6` | Ask price trend volatility level 6 | `mse(AskPrice6 ORDER BY TimestampNanoseconds) / mid(AskPrice6)` |
| `ask_price_trend_vol_l7` | Ask price trend volatility level 7 | `mse(AskPrice7 ORDER BY TimestampNanoseconds) / mid(AskPrice7)` |
| `ask_price_trend_vol_l8` | Ask price trend volatility level 8 | `mse(AskPrice8 ORDER BY TimestampNanoseconds) / mid(AskPrice8)` |
| `ask_price_trend_vol_l9` | Ask price trend volatility level 9 | `mse(AskPrice9 ORDER BY TimestampNanoseconds) / mid(AskPrice9)` |
| `ask_price_trend_vol_l10` | Ask price trend volatility level 10 | `mse(AskPrice10 ORDER BY TimestampNanoseconds) / mid(AskPrice10)` |
| `mid_price_trend_vol_l1` | Mid price trend volatility level 1 | `mse((BidPrice1 + AskPrice1) / 2 ORDER BY TimestampNanoseconds) / mid((BidPrice1 + AskPrice1) / 2)` |
| `mid_price_trend_vol_l2` | Mid price trend volatility level 2 | `mse((BidPrice2 + AskPrice2) / 2 ORDER BY TimestampNanoseconds) / mid((BidPrice2 + AskPrice2) / 2)` |
| `mid_price_trend_vol_l3` | Mid price trend volatility level 3 | `mse((BidPrice3 + AskPrice3) / 2 ORDER BY TimestampNanoseconds) / mid((BidPrice3 + AskPrice3) / 2)` |
| `mid_price_trend_vol_l4` | Mid price trend volatility level 4 | `mse((BidPrice4 + AskPrice4) / 2 ORDER BY TimestampNanoseconds) / mid((BidPrice4 + AskPrice4) / 2)` |
| `mid_price_trend_vol_l5` | Mid price trend volatility level 5 | `mse((BidPrice5 + AskPrice5) / 2 ORDER BY TimestampNanoseconds) / mid((BidPrice5 + AskPrice5) / 2)` |
| `mid_price_trend_vol_l6` | Mid price trend volatility level 6 | `mse((BidPrice6 + AskPrice6) / 2 ORDER BY TimestampNanoseconds) / mid((BidPrice6 + AskPrice6) / 2)` |
| `mid_price_trend_vol_l7` | Mid price trend volatility level 7 | `mse((BidPrice7 + AskPrice7) / 2 ORDER BY TimestampNanoseconds) / mid((BidPrice7 + AskPrice7) / 2)` |
| `mid_price_trend_vol_l8` | Mid price trend volatility level 8 | `mse((BidPrice8 + AskPrice8) / 2 ORDER BY TimestampNanoseconds) / mid((BidPrice8 + AskPrice8) / 2)` |
| `mid_price_trend_vol_l9` | Mid price trend volatility level 9 | `mse((BidPrice9 + AskPrice9) / 2 ORDER BY TimestampNanoseconds) / mid((BidPrice9 + AskPrice9) / 2)` |
| `mid_price_trend_vol_l10` | Mid price trend volatility level 10 | `mse((BidPrice10 + AskPrice10) / 2 ORDER BY TimestampNanoseconds) / mid((BidPrice10 + AskPrice10) / 2)` |
| `bid_quantity_trend_vol_l1` | Bid quantity trend volatility level 1 | `mse(BidQuantity1 ORDER BY TimestampNanoseconds) / mid(BidQuantity1)` |
| `bid_quantity_trend_vol_l2` | Bid quantity trend volatility level 2 | `mse(BidQuantity2 ORDER BY TimestampNanoseconds) / mid(BidQuantity2)` |
| `bid_quantity_trend_vol_l3` | Bid quantity trend volatility level 3 | `mse(BidQuantity3 ORDER BY TimestampNanoseconds) / mid(BidQuantity3)` |
| `bid_quantity_trend_vol_l4` | Bid quantity trend volatility level 4 | `mse(BidQuantity4 ORDER BY TimestampNanoseconds) / mid(BidQuantity4)` |
| `bid_quantity_trend_vol_l5` | Bid quantity trend volatility level 5 | `mse(BidQuantity5 ORDER BY TimestampNanoseconds) / mid(BidQuantity5)` |
| `bid_quantity_trend_vol_l6` | Bid quantity trend volatility level 6 | `mse(BidQuantity6 ORDER BY TimestampNanoseconds) / mid(BidQuantity6)` |
| `bid_quantity_trend_vol_l7` | Bid quantity trend volatility level 7 | `mse(BidQuantity7 ORDER BY TimestampNanoseconds) / mid(BidQuantity7)` |
| `bid_quantity_trend_vol_l8` | Bid quantity trend volatility level 8 | `mse(BidQuantity8 ORDER BY TimestampNanoseconds) / mid(BidQuantity8)` |
| `bid_quantity_trend_vol_l9` | Bid quantity trend volatility level 9 | `mse(BidQuantity9 ORDER BY TimestampNanoseconds) / mid(BidQuantity9)` |
| `bid_quantity_trend_vol_l10` | Bid quantity trend volatility level 10 | `mse(BidQuantity10 ORDER BY TimestampNanoseconds) / mid(BidQuantity10)` |
| `ask_quantity_trend_vol_l1` | Ask quantity trend volatility level 1 | `mse(AskQuantity1 ORDER BY TimestampNanoseconds) / mid(AskQuantity1)` |
| `ask_quantity_trend_vol_l2` | Ask quantity trend volatility level 2 | `mse(AskQuantity2 ORDER BY TimestampNanoseconds) / mid(AskQuantity2)` |
| `ask_quantity_trend_vol_l3` | Ask quantity trend volatility level 3 | `mse(AskQuantity3 ORDER BY TimestampNanoseconds) / mid(AskQuantity3)` |
| `ask_quantity_trend_vol_l4` | Ask quantity trend volatility level 4 | `mse(AskQuantity4 ORDER BY TimestampNanoseconds) / mid(AskQuantity4)` |
| `ask_quantity_trend_vol_l5` | Ask quantity trend volatility level 5 | `mse(AskQuantity5 ORDER BY TimestampNanoseconds) / mid(AskQuantity5)` |
| `ask_quantity_trend_vol_l6` | Ask quantity trend volatility level 6 | `mse(AskQuantity6 ORDER BY TimestampNanoseconds) / mid(AskQuantity6)` |
| `ask_quantity_trend_vol_l7` | Ask quantity trend volatility level 7 | `mse(AskQuantity7 ORDER BY TimestampNanoseconds) / mid(AskQuantity7)` |
| `ask_quantity_trend_vol_l8` | Ask quantity trend volatility level 8 | `mse(AskQuantity8 ORDER BY TimestampNanoseconds) / mid(AskQuantity8)` |
| `ask_quantity_trend_vol_l9` | Ask quantity trend volatility level 9 | `mse(AskQuantity9 ORDER BY TimestampNanoseconds) / mid(AskQuantity9)` |
| `ask_quantity_trend_vol_l10` | Ask quantity trend volatility level 10 | `mse(AskQuantity10 ORDER BY TimestampNanoseconds) / mid(AskQuantity10)` |
| `bid_volume_trend_vol_l1` | Bid volume trend volatility level 1 | `mse(BidPrice1 * BidQuantity1 ORDER BY TimestampNanoseconds) / mid(BidPrice1 * BidQuantity1)` |
| `bid_volume_trend_vol_l2` | Bid volume trend volatility level 2 | `mse(BidPrice2 * BidQuantity2 ORDER BY TimestampNanoseconds) / mid(BidPrice2 * BidQuantity2)` |
| `bid_volume_trend_vol_l3` | Bid volume trend volatility level 3 | `mse(BidPrice3 * BidQuantity3 ORDER BY TimestampNanoseconds) / mid(BidPrice3 * BidQuantity3)` |
| `bid_volume_trend_vol_l4` | Bid volume trend volatility level 4 | `mse(BidPrice4 * BidQuantity4 ORDER BY TimestampNanoseconds) / mid(BidPrice4 * BidQuantity4)` |
| `bid_volume_trend_vol_l5` | Bid volume trend volatility level 5 | `mse(BidPrice5 * BidQuantity5 ORDER BY TimestampNanoseconds) / mid(BidPrice5 * BidQuantity5)` |
| `bid_volume_trend_vol_l6` | Bid volume trend volatility level 6 | `mse(BidPrice6 * BidQuantity6 ORDER BY TimestampNanoseconds) / mid(BidPrice6 * BidQuantity6)` |
| `bid_volume_trend_vol_l7` | Bid volume trend volatility level 7 | `mse(BidPrice7 * BidQuantity7 ORDER BY TimestampNanoseconds) / mid(BidPrice7 * BidQuantity7)` |
| `bid_volume_trend_vol_l8` | Bid volume trend volatility level 8 | `mse(BidPrice8 * BidQuantity8 ORDER BY TimestampNanoseconds) / mid(BidPrice8 * BidQuantity8)` |
| `bid_volume_trend_vol_l9` | Bid volume trend volatility level 9 | `mse(BidPrice9 * BidQuantity9 ORDER BY TimestampNanoseconds) / mid(BidPrice9 * BidQuantity9)` |
| `bid_volume_trend_vol_l10` | Bid volume trend volatility level 10 | `mse(BidPrice10 * BidQuantity10 ORDER BY TimestampNanoseconds) / mid(BidPrice10 * BidQuantity10)` |
| `ask_volume_trend_vol_l1` | Ask volume trend volatility level 1 | `mse(AskPrice1 * AskQuantity1 ORDER BY TimestampNanoseconds) / mid(AskPrice1 * AskQuantity1)` |
| `ask_volume_trend_vol_l2` | Ask volume trend volatility level 2 | `mse(AskPrice2 * AskQuantity2 ORDER BY TimestampNanoseconds) / mid(AskPrice2 * AskQuantity2)` |
| `ask_volume_trend_vol_l3` | Ask volume trend volatility level 3 | `mse(AskPrice3 * AskQuantity3 ORDER BY TimestampNanoseconds) / mid(AskPrice3 * AskQuantity3)` |
| `ask_volume_trend_vol_l4` | Ask volume trend volatility level 4 | `mse(AskPrice4 * AskQuantity4 ORDER BY TimestampNanoseconds) / mid(AskPrice4 * AskQuantity4)` |
| `ask_volume_trend_vol_l5` | Ask volume trend volatility level 5 | `mse(AskPrice5 * AskQuantity5 ORDER BY TimestampNanoseconds) / mid(AskPrice5 * AskQuantity5)` |
| `ask_volume_trend_vol_l6` | Ask volume trend volatility level 6 | `mse(AskPrice6 * AskQuantity6 ORDER BY TimestampNanoseconds) / mid(AskPrice6 * AskQuantity6)` |
| `ask_volume_trend_vol_l7` | Ask volume trend volatility level 7 | `mse(AskPrice7 * AskQuantity7 ORDER BY TimestampNanoseconds) / mid(AskPrice7 * AskQuantity7)` |
| `ask_volume_trend_vol_l8` | Ask volume trend volatility level 8 | `mse(AskPrice8 * AskQuantity8 ORDER BY TimestampNanoseconds) / mid(AskPrice8 * AskQuantity8)` |
| `ask_volume_trend_vol_l9` | Ask volume trend volatility level 9 | `mse(AskPrice9 * AskQuantity9 ORDER BY TimestampNanoseconds) / mid(AskPrice9 * AskQuantity9)` |
| `ask_volume_trend_vol_l10` | Ask volume trend volatility level 10 | `mse(AskPrice10 * AskQuantity10 ORDER BY TimestampNanoseconds) / mid(AskPrice10 * AskQuantity10)` |

