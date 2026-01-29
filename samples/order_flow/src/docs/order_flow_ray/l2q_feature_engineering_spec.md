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
| `spread_bps_l1` | Spread in basis points level 1 | `mid((AskPrice1 - BidPrice1) / ((BidPrice1 + AskPrice1) / 2) * 10000)` |
| `spread_bps_l2` | Spread in basis points level 2 | `mid((AskPrice2 - BidPrice2) / ((BidPrice2 + AskPrice2) / 2) * 10000)` |
| `spread_bps_l3` | Spread in basis points level 3 | `mid((AskPrice3 - BidPrice3) / ((BidPrice3 + AskPrice3) / 2) * 10000)` |
| `spread_bps_l4` | Spread in basis points level 4 | `mid((AskPrice4 - BidPrice4) / ((BidPrice4 + AskPrice4) / 2) * 10000)` |
| `spread_bps_l5` | Spread in basis points level 5 | `mid((AskPrice5 - BidPrice5) / ((BidPrice5 + AskPrice5) / 2) * 10000)` |
| `spread_bps_l6` | Spread in basis points level 6 | `mid((AskPrice6 - BidPrice6) / ((BidPrice6 + AskPrice6) / 2) * 10000)` |
| `spread_bps_l7` | Spread in basis points level 7 | `mid((AskPrice7 - BidPrice7) / ((BidPrice7 + AskPrice7) / 2) * 10000)` |
| `spread_bps_l8` | Spread in basis points level 8 | `mid((AskPrice8 - BidPrice8) / ((BidPrice8 + AskPrice8) / 2) * 10000)` |
| `spread_bps_l9` | Spread in basis points level 9 | `mid((AskPrice9 - BidPrice9) / ((BidPrice9 + AskPrice9) / 2) * 10000)` |
| `spread_bps_l10` | Spread in basis points level 10 | `mid((AskPrice10 - BidPrice10) / ((BidPrice10 + AskPrice10) / 2) * 10000)` |

### 4. Quantity Features (Level 1)
| Feature | Description | Calculation |
|---------|-------------|-------------|
| `bid_quantity_mean` | Average bid quantity | `mean(BidQuantity1)` |
| `bid_quantity_max` | Maximum bid quantity | `max(BidQuantity1)` |
| `bid_quantity_min` | Minimum bid quantity | `min(BidQuantity1)` |
| `ask_quantity_mean` | Average ask quantity | `mean(AskQuantity1)` |
| `ask_quantity_max` | Maximum ask quantity | `max(AskQuantity1)` |
| `ask_quantity_min` | Minimum ask quantity | `min(AskQuantity1)` |

### 5. Order Count Features (Level 1)
| Feature | Description | Calculation |
|---------|-------------|-------------|
| `bid_orders_mean` | Average bid order count | `mean(BidNumOrders1)` |
| `bid_orders_max` | Maximum bid order count | `max(BidNumOrders1)` |
| `ask_orders_mean` | Average ask order count | `mean(AskNumOrders1)` |
| `ask_orders_max` | Maximum ask order count | `max(AskNumOrders1)` |

### 6. Imbalance Features
| Feature | Description | Calculation |
|---------|-------------|-------------|
| `quantity_imbalance_mean` | Average quantity imbalance | `mean(BidQuantity1 - AskQuantity1)` |
| `quantity_imbalance_ratio_mean` | Average quantity imbalance ratio | `mean((BidQuantity1 - AskQuantity1) / (BidQuantity1 + AskQuantity1))` |
| `order_imbalance_mean` | Average order count imbalance | `mean(BidNumOrders1 - AskNumOrders1)` |
| `order_imbalance_ratio_mean` | Average order imbalance ratio | `mean((BidNumOrders1 - AskNumOrders1) / (BidNumOrders1 + AskNumOrders1))` |

### 7. Depth Features (Multi-level)
| Feature | Description | Calculation |
|---------|-------------|-------------|
| `total_bid_quantity_l5` | Total bid quantity (levels 1-5) | `mean(sum(BidQuantity1-5))` |
| `total_ask_quantity_l5` | Total ask quantity (levels 1-5) | `mean(sum(AskQuantity1-5))` |
| `total_bid_orders_l5` | Total bid orders (levels 1-5) | `mean(sum(BidNumOrders1-5))` |
| `total_ask_orders_l5` | Total ask orders (levels 1-5) | `mean(sum(AskNumOrders1-5))` |
| `weighted_bid_price_l5` | Quantity-weighted bid price (levels 1-5) | `mean(sum(BidPrice1-5 * BidQuantity1-5) / sum(BidQuantity1-5))` |
| `weighted_ask_price_l5` | Quantity-weighted ask price (levels 1-5) | `mean(sum(AskPrice1-5 * AskQuantity1-5) / sum(AskQuantity1-5))` |asis points level 6 | `mean((AskPrice6 - BidPrice6) / ((BidPrice6 + AskPrice6) / 2) * 10000)` |
| `spread_bps_l7` | Spread in basis points level 7 | `mean((AskPrice7 - BidPrice7) / ((BidPrice7 + AskPrice7) / 2) * 10000)` |
| `spread_bps_l8` | Spread in basis points level 8 | `mean((AskPrice8 - BidPrice8) / ((BidPrice8 + AskPrice8) / 2) * 10000)` |
| `spread_bps_l9` | Spread in basis points level 9 | `mean((AskPrice9 - BidPrice9) / ((BidPrice9 + AskPrice9) / 2) * 10000)` |
| `spread_bps_l10` | Spread in basis points level 10 | `mean((AskPrice10 - BidPrice10) / ((BidPrice10 + AskPrice10) / 2) * 10000)` |

### 8. Volatility Features
| Feature | Description | Calculation |
|---------|-------------|-------------|
| `mid_price_volatility` | Mid-price volatility | `std((BidPrice1 + AskPrice1) / 2)` |
| `spread_volatility` | Spread volatility | `std(AskPrice1 - BidPrice1)` |
| `bid_price_volatility` | Bid price volatility | `std(BidPrice1)` |
| `ask_price_volatility` | Ask price volatility | `std(AskPrice1)` |------------|-------------|
| `mid_price_volatility` | Mid-price volatility | `std((BidPrice1 + AskPrice1) / 2)` |
| `spread_volatility` | Spread volatility | `std(AskPrice1 - BidPrice1)` |
| `bid_price_volatility` | Bid price volatility | `std(BidPrice1)` |
| `ask_price_volatility` | Ask price volatility | `std(AskPrice1)` |

## Implementation Priority

### Phase 1 (Core Features)
- Bar metadata, quote activity, top-of-book prices, basic spreads

### Phase 2 (Enhanced Features)  
- Quantity/order features, imbalance metrics, volatility

### Phase 3 (Advanced Features)
- Multi-level depth features, weighted prices

## Output Schema Estimate

- **Total Columns**: ~70-80 columns
- **Grouping Columns**: 7 (bar_id, TradeDate, Ticker, ISOExchangeCode, MIC, ExchangeTicker, market_state_mode)
- **Feature Columns**: ~65-75 features

## Key Differences from Trade Features

1. **No Aggressor Side**: L2Q data doesn't have trade direction
2. **Multi-level Data**: 10 levels of bid/ask data vs single trade price
3. **Order Book Focus**: Liquidity and depth vs executed volume
4. **Market Structure**: Spread and imbalance vs trade flow
5. **Volatility Metrics**: Price movement vs trade impact