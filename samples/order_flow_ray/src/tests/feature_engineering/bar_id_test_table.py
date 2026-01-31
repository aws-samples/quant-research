"""Generate test tables for bar_id calculation with multiple bar durations."""
import polars as pl
import os

# Sample timestamps from CSV (TradeTimestampNanoseconds column)
test_data = [
    1709125940444277167,  # 2024-02-28T13:12:20.444277167
    1709126016935191734,  # 2024-02-28T13:13:36.935191734
    1709126338369834464,  # 2024-02-28T13:18:58.369834464
    1709126762577066338,  # 2024-02-28T13:26:02.577066338
    1709127269468259761,  # 2024-02-28T13:34:29.468259761
    1709127270475125287,  # 2024-02-28T13:34:30.475125287
    1709127279305566711,  # 2024-02-28T13:34:39.305566711
    1709128080955723899,  # 2024-02-28T13:48:00.955723899
    1709128200913918878,  # 2024-02-28T13:50:00.913918878
    1709129160905128742,  # 2024-02-28T14:06:00.905128742
]

bar_durations = [50, 100, 200, 250, 500, 1000, 5000, 10000]
output_dir = '/Users/blitvin/IdeaProjects/quant-research-sample-using-amazon-ecs-and-aws-batch/samples/order_flow_ray/src/tests/feature_engineering/test_data'

for bar_duration_ms in bar_durations:
    # Calculate expected bar_id for each timestamp
    expected_bar_ids = []
    for ts_ns in test_data:
        ts_ms = ts_ns / 1_000_000  # Convert nanoseconds to milliseconds
        bar_id = int((ts_ms / bar_duration_ms).__ceil__() * bar_duration_ms)
        expected_bar_ids.append(bar_id)
    
    # Create table with human readable timestamps
    df = pl.DataFrame({
        'input_timestamp_ns': test_data,
        'input_timestamp_readable': pl.from_epoch(pl.Series(test_data), time_unit='ns'),
        'expected_bar_id_ms': expected_bar_ids,
        'expected_bar_id_readable': pl.from_epoch(pl.Series(expected_bar_ids), time_unit='ms')
    })
    
    # Save to CSV
    filename = f'bar_id_test_table_{bar_duration_ms}ms.csv'
    filepath = os.path.join(output_dir, filename)
    df.write_csv(filepath)
    
    print(f"Generated {filename} for {bar_duration_ms}ms bar duration")

print(f"\nAll test tables saved to {output_dir}")