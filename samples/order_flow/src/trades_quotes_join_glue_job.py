# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: MIT-0

import sys
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql import functions as f
from tempo.tsdf import TSDF

# Initialize Glue context
args = getResolvedOptions(sys.argv, ["JOB_NAME", "output_database", "output_table"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Job parameters
output_database = args["output_database"]
output_table = args["output_table"]

# Optimized Spark configuration for TSDF operations
spark.conf.set("spark.sql.adaptive.enabled", "true")
spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")
spark.conf.set("spark.sql.adaptive.skewJoin.enabled", "true")
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "100MB")
spark.conf.set("spark.sql.shuffle.partitions", "200")
spark.conf.set("spark.sql.adaptive.localShuffleReader.enabled", "true")

# Enhanced Iceberg-specific configurations for hidden partitioning
spark.conf.set("spark.sql.iceberg.vectorization.enabled", "true")
spark.conf.set("spark.sql.parquet.compression.codec", "snappy")
spark.conf.set("spark.sql.parquet.block.size", "268435456")  # 256MB blocks
spark.conf.set("spark.sql.files.maxPartitionBytes", "268435456")  # 256MB partitions
spark.conf.set(
    "spark.sql.adaptive.advisoryPartitionSizeInBytes", "268435456"
)  # 256MB advisory size

# Hidden partitioning specific optimizations
spark.conf.set("spark.sql.iceberg.planning.preserve-data-grouping", "true")
spark.conf.set("spark.sql.iceberg.check-ordering", "false")

print("Starting optimized Glue job with TSDF and Iceberg hidden partitioning...")

# Check if target table exists and create it if needed with hidden partitioning
try:
    existing_tables = spark.sql(f"SHOW TABLES IN {output_database}").collect()
    table_exists = any(row.tableName == output_table for row in existing_tables)
    if table_exists:
        print(f"Table {output_database}.{output_table} exists")
    else:
        raise Exception("Table does not exist")
except Exception:
    print(
        f"Creating optimized table {output_database}.{output_table} with hidden partitioning"
    )
    spark.sql(
        f"""
    CREATE TABLE IF NOT EXISTS {output_database}.{output_table} (
        exchange_id INT,
        exchange_code STRING,
        instrument INT,
        trade_timestamp TIMESTAMP,
        side INT,
        trade_price DOUBLE,
        trade_quantity DOUBLE,
        tradeid STRING,
        exchangetradetimestamp BIGINT,
        base_underlying_code STRING,
        counter_underlying_code STRING,
        quote_timestamp TIMESTAMP,
        bid INT,
        bid_price DOUBLE,
        bid_quantity DOUBLE,
        bid_ordercount INT,
        ask INT,
        ask_price DOUBLE,
        ask_quantity DOUBLE,
        ask_ordercount INT,
        midprice DOUBLE,
        order_flow DOUBLE,
        trade_date STRING
    )
    USING ICEBERG
    PARTITIONED BY (
        -- Hidden partitioning using hours for time-based optimization (includes daily)
        hours(trade_timestamp),
        -- Hash partitioning for instrument to ensure even distribution
        bucket(16, instrument),
        -- Hash partitioning for exchange_id for cross-exchange analysis
        bucket(8, exchange_id)
    )
    TBLPROPERTIES (
        'write.target-file-size-bytes'='268435456',
        'write.parquet.compression-codec'='snappy',
        'write.distribution-mode'='hash',
        'write.wap.enabled'='true',
        'format-version'='2',
        'write.delete.mode'='merge-on-read',
        'write.update.mode'='merge-on-read',
        'write.object-storage.enabled'='true',
        'write.object-storage.path'='s3://your-bucket/iceberg-data/',
        'write.metadata.delete-after-commit.enabled'='true',
        'write.metadata.previous-versions-max'='5',
        'write.spark.fanout.enabled'='true'
    )
    """
    )

# Get the most liquid instruments to filter data
print("Identifying top liquid instruments...")
try:
    liquid_instruments_df = (
        spark.table("crypto_db.most_liquid_instruments").orderBy("rank").limit(20)
    )
    print(f"Found liquid instruments table")
except Exception as e:
    print(f"Error getting liquid instruments: {e}")
    # Fallback: get top instruments by volume
    liquid_instruments_df = spark.sql(
        """
        SELECT DISTINCT instrument, exchange_id, 
               FIRST(exchange_code) as exchange_code
        FROM crypto.trades 
        GROUP BY instrument, exchange_id
        ORDER BY COUNT(*) DESC
        LIMIT 20
    """
    )

# Register liquid instruments as temp view for join
liquid_instruments_df.createOrReplaceTempView("liquid_instruments")

# Create a list of instrument-exchange pairs for filtering
liquid_instruments_list = liquid_instruments_df.select(
    "instrument", "exchange_id"
).collect()
trades_filter = " OR ".join(
    [
        f"(t.instrument = {row.instrument} AND t.exchange_id = {row.exchange_id})"
        for row in liquid_instruments_list
    ]
)
quotes_filter = " OR ".join(
    [
        f"(instrument = {row.instrument} AND exchange_id = {row.exchange_id})"
        for row in liquid_instruments_list
    ]
)

print(f"Processing {len(liquid_instruments_list)} liquid instruments in batch...")

# Load all trades data for liquid instruments with optimized timestamp handling
print("Loading trades data for all liquid instruments...")
trades_df = spark.sql(
    f"""
    SELECT 
        t.exchange_id,
        COALESCE(li.exchange_code, CONCAT('exchange_', t.exchange_id)) as exchange_code,
        t.instrument,
        t.adaptertimestamp_ts_utc,
        t.side,
        CAST(t.price as DOUBLE) as price,
        CAST(t.quantity as DOUBLE) as quantity,
        t.tradeid,
        t.exchangetradetimestamp,
        t.base_underlying_code,
        t.counter_underlying_code,
        -- Add derived time-based columns for partitioning optimization
        date(t.adaptertimestamp_ts_utc) as date,
        hour(t.adaptertimestamp_ts_utc) as hour,
        date_trunc('hour', t.adaptertimestamp_ts_utc) as hour_partition
    FROM crypto.trades t
    LEFT JOIN liquid_instruments li
    ON t.instrument = li.instrument AND t.exchange_id = li.exchange_id
    WHERE ({trades_filter})
    AND t.adaptertimestamp_ts_utc IS NOT NULL
"""
)

# Load all quotes data for liquid instruments with time-based optimization
print("Loading quotes data for all liquid instruments...")
quotes_df = spark.sql(
    f"""
    SELECT 
        instrument,
        exchange_id,
        adaptertimestamp_ts_utc,
        CAST(bid as INT) as bid,
        CAST(bid_price as DOUBLE) as bid_price,
        CAST(bid_quantity as DOUBLE) as bid_quantity,
        CAST(bid_ordercount as INT) as bid_ordercount,
        CAST(ask as INT) as ask,
        CAST(ask_price as DOUBLE) as ask_price,
        CAST(ask_quantity as DOUBLE) as ask_quantity,
        CAST(ask_ordercount as INT) as ask_ordercount,
        -- Add derived time-based columns for partitioning optimization
        date(adaptertimestamp_ts_utc) as date,
        hour(adaptertimestamp_ts_utc) as hour,
        date_trunc('hour', adaptertimestamp_ts_utc) as hour_partition
    FROM crypto.top_of_book
    WHERE ({quotes_filter})
    AND adaptertimestamp_ts_utc IS NOT NULL
    AND bid_price IS NOT NULL
    AND ask_price IS NOT NULL
"""
)

# Check data availability
trades_count = trades_df.count()
quotes_count = quotes_df.count()

print(
    f"Loaded {trades_count} trades and {quotes_count} quotes for all liquid instruments"
)

if trades_count == 0 or quotes_count == 0:
    raise Exception("No data found for liquid instruments")

# Create TSDF objects with enhanced partitioning for better performance
print("Creating TSDF objects with time-based partitioning...")
trades_tsdf = TSDF(
    trades_df,
    ts_col="adaptertimestamp_ts_utc",
    partition_cols=["date", "hour", "instrument", "exchange_id"],
)

quotes_tsdf = TSDF(
    quotes_df,
    ts_col="adaptertimestamp_ts_utc",
    partition_cols=["date", "hour", "instrument", "exchange_id"],
)

# Perform as-of join using TSDF with optimized partitioning
print("Performing as-of join for all instruments with time-based optimization...")
result_tsdf = trades_tsdf.asofJoin(
    quotes_tsdf,
    right_prefix="quote_",
    tsPartitionVal=None,  # Let TSDF handle partitioning automatically
    fraction=1.0,  # Use all data
)

# Process the joined data with optimized column selection
print("Processing joined results with hidden partitioning optimization...")
result_df = result_tsdf.df

# Add calculated columns and prepare for hidden partitioning
processed_df = (
    result_df.select(
        f.col("exchange_id"),
        f.col("exchange_code"),
        f.col("instrument"),
        f.col("adaptertimestamp_ts_utc").alias("trade_timestamp"),
        f.col("side"),
        f.col("price").alias("trade_price"),
        f.col("quantity").alias("trade_quantity"),
        f.col("tradeid"),
        f.col("exchangetradetimestamp"),
        f.col("base_underlying_code"),
        f.col("counter_underlying_code"),
        f.col("quote__adaptertimestamp_ts_utc").alias("quote_timestamp"),
        f.col("quote__bid").alias("bid"),
        f.col("quote__bid_price").alias("bid_price"),
        f.col("quote__bid_quantity").alias("bid_quantity"),
        f.col("quote__bid_ordercount").alias("bid_ordercount"),
        f.col("quote__ask").alias("ask"),
        f.col("quote__ask_price").alias("ask_price"),
        f.col("quote__ask_quantity").alias("ask_quantity"),
        f.col("quote__ask_ordercount").alias("ask_ordercount"),
        f.col("date").alias("trade_date"),
    )
    .withColumn("midprice", (f.col("bid_price") + f.col("ask_price")) / 2)
    .withColumn(
        "order_flow",
        f.when(f.col("side") == 0, f.col("trade_quantity")).otherwise(
            -f.col("trade_quantity")
        ),
    )
    .fillna(0)
)  # Fill null values with 0

# Cache the result for better performance
processed_df = processed_df.cache()

# Optimize data distribution for hidden partitioning
print("Optimizing data distribution for hidden partitioning...")

# Pre-sort and distribute data to align with hidden partitioning scheme
# This helps Iceberg create optimal file layout
optimized_df = processed_df.repartition(
    f.col("trade_timestamp"),  # Time-based distribution
    f.col("instrument"),  # Instrument-based distribution
    f.col("exchange_id"),  # Exchange-based distribution
).sortWithinPartitions(
    f.col("trade_timestamp"),  # Sort by time within partitions
    f.col("instrument"),  # Then by instrument
    f.col("exchange_id"),  # Then by exchange
)

# Write results using Iceberg format with hidden partitioning optimization
print("Writing results to optimized Iceberg table with hidden partitioning...")
optimized_df.write.format("iceberg").mode("overwrite").option(
    "write-audit-publish", "true"
).option("check-nullability", "false").option("fanout-enabled", "true").option(
    "distribution-mode", "hash"
).option(
    "write.distribution.mode", "hash"
).saveAsTable(
    f"{output_database}.{output_table}"
)

# Perform enhanced Iceberg table optimization for hidden partitioning
print("Performing enhanced Iceberg table optimization...")

try:
    # Rewrite data files with hidden partition awareness
    spark.sql(
        f"""
        CALL glue_catalog.system.rewrite_data_files(
            table => '{output_database}.{output_table}',
            strategy => 'binpack',
            options => map(
                'target-file-size-bytes', '268435456',
                'min-file-size-bytes', '134217728',
                'rewrite-all', 'true'
            )
        )
    """
    )
    print("Data files optimized for hidden partitioning successfully")
except Exception as e:
    print(f"Warning: Data file optimization failed: {e}")

try:
    # Rewrite manifest files for better query performance with hidden partitioning
    spark.sql(
        f"""
        CALL glue_catalog.system.rewrite_manifests(
            table => '{output_database}.{output_table}'
        )
    """
    )
    print("Manifest files optimized for hidden partitioning successfully")
except Exception as e:
    print(f"Warning: Manifest optimization failed: {e}")

try:
    # Optimize table for better scan performance
    spark.sql(
        f"""
        CALL glue_catalog.system.expire_snapshots(
            table => '{output_database}.{output_table}',
            older_than => TIMESTAMP '2024-01-01 00:00:00'
        )
    """
    )
    print("Expired old snapshots successfully")
except Exception as e:
    print(f"Warning: Snapshot expiration failed: {e}")

# Display partition information for verification
print("Displaying partition information for hidden partitioning:")
try:
    partition_info = spark.sql(
        f"""
        SELECT 
            partition,
            record_count,
            file_count,
            total_size
        FROM {output_database}.{output_table}.partitions
        ORDER BY partition
        LIMIT 10
    """
    )
    partition_info.show(10, False)
except Exception as e:
    print(f"Could not display partition info: {e}")

# Clean up optimized dataframe
optimized_df.unpersist()

# Get final statistics
result_count = processed_df.count()
print(f"Successfully processed {result_count} total records with hidden partitioning")

# Display table properties to verify hidden partitioning configuration
try:
    table_props = spark.sql(f"DESCRIBE TABLE EXTENDED {output_database}.{output_table}")
    print("Table properties with hidden partitioning:")
    table_props.filter(f.col("col_name").like("%partition%")).show(20, False)
except Exception as e:
    print(f"Could not display table properties: {e}")

# Clean up cache and temp views
processed_df.unpersist()
spark.catalog.dropTempView("liquid_instruments")

print(
    "Enhanced TSDF-based Glue job with Iceberg hidden partitioning completed successfully!"
)

# Final statistics and performance metrics
try:
    final_stats = spark.sql(
        f"""
        SELECT 
            COUNT(*) as total_records,
            COUNT(DISTINCT instrument) as distinct_instruments,
            COUNT(DISTINCT exchange_id) as distinct_exchanges,
            MIN(trade_timestamp) as min_timestamp,
            MAX(trade_timestamp) as max_timestamp,
            COUNT(DISTINCT DATE(trade_timestamp)) as distinct_days
        FROM {output_database}.{output_table}
    """
    ).collect()[0]

    print(f"Final Statistics:")
    print(f"- Total records: {final_stats.total_records}")
    print(f"- Distinct instruments: {final_stats.distinct_instruments}")
    print(f"- Distinct exchanges: {final_stats.distinct_exchanges}")
    print(f"- Date range: {final_stats.min_timestamp} to {final_stats.max_timestamp}")
    print(f"- Distinct days: {final_stats.distinct_days}")

except Exception as e:
    print(f"Could not get final statistics: {e}")

job.commit()
