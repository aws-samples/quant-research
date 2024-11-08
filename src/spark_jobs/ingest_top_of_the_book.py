import sys
from pyspark.sql import SparkSession
from pyspark.sql import functions as f

def read_file(sample_file):
    return (
        spark.read
        .option("compression", "org.apache.spark.io.ZStdCompressionCodec")
        .text(sample_file)
    )

def parse(df_):
    return (
        df_.select(f.from_json(f.col("value"), "array<string>").alias("value"))
        .where(f.col("value").getItem(0).isin([1,2,6]))
        .select(
            f.col("value").getItem(0).alias("msgType"),
            f.col("value").getItem(1).alias("instrument"),
            f.col("value").getItem(2).alias("prevEventId"),
            f.col("value").getItem(3).alias("eventId"),
            f.col("value").getItem(4).alias("adapterTimestamp"),
            f.col("value").getItem(5).alias("exchangeTimestamp"),
            f.from_json(f.col("value").getItem(6), "array<array<string>>").alias("payload")
        )
    )


if __name__ == "__main__":

    print(len(sys.argv))
    if (len(sys.argv) != 3):
        print("Usage: spark-etl [input-data] [master-data] [target-bucket]")
        sys.exit(0)

    spark = SparkSession \
        .builder \
        .appName("IngestCryptoStructTopOfTheBook") \
        .getOrCreate()


    top_of_book = read_file(sys.argv[1]).filter(f.col("value").startswith("[6"))
    top_of_book_parsed = parse(top_of_book)
    master_data = spark.read.parquet(sys.argv[2])
    target_bucket = sys.argv[3]

    top_of_book_parsed_2 = (
        top_of_book_parsed.select(
            "instrument",
            (f.col(f'adapterTimestamp')/1000/1000/1000).cast("timestamp").alias("adapterTimestamp_ts_utc"),
            f.col("payload").getItem(0).getItem(0).cast("int").alias("bid"),
            f.col("payload").getItem(0).getItem(1).cast("float").alias("bid_price"),
            f.col("payload").getItem(0).getItem(2).cast("float").alias("bid_quantity"),
            f.col("payload").getItem(0).getItem(3).cast("string").alias("bid_ordercount"),

            f.col("payload").getItem(1).getItem(0).cast("int").alias("ask"),
            f.col("payload").getItem(1).getItem(1).cast("float").alias("ask_price"),
            f.col("payload").getItem(1).getItem(2).cast("float").alias("ask_quantity"),
            f.col("payload").getItem(1).getItem(3).cast("string").alias("ask_ordercount"),
        )
    )
    top_of_book_parsed_3 = (
        top_of_book_parsed_2.join(f.broadcast(master_data),(top_of_book_parsed_2.instrument==master_data.id))
        .drop(master_data.id)
        .select(
            'exchange_id',
            'exchange_code',
            'instrument',
            'adapterTimestamp_ts_utc',
            'bid',
            'bid_price',
            'bid_quantity',
            'bid_ordercount',
            'ask',
            'ask_price',
            'ask_quantity',
            'ask_ordercount'
        )
    )
    temp_view = "top_of_book"
    top_of_book_parsed_3.createOrReplaceTempView(temp_view)
    schema_json = top_of_book_parsed_3.schema.json()
    ddl = spark.sparkContext._jvm.org.apache.spark.sql.types.DataType.fromJson(schema_json).toDDL()
    spark.sql(f"CREATE DATABASE IF NOT EXISTS crypto LOCATION 's3://{target_bucket}/'")
    table_name = "glue_catalog.crypto.top_of_book"

    # 134217728 bytes = 128MB
    # 536870912 bytes = 512MB
    # merge-on-read vs copy-on-write:
    #  - write.delete.mode = Mode used for delete commands
    #  - write.update.mode = Mode used for update commands
    #  - write.merge.mode = Mode used for merge commands
    #
    spark.sql(f"DROP TABLE IF EXISTS {table_name} PURGE")
    spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {table_name} ({ddl})
    USING iceberg
    OPTIONS ('format-version'='2')
    LOCATION 's3://{target_bucket}/iceberg/'
    PARTITIONED BY (exchange_code, instrument)
    TBLPROPERTIES (
        'table_type'='iceberg',
        'write.merge.mode'='copy-on-write',
        'write.target-file-size-bytes'='536870912',
        'write.distribution-mode' = 'range',
        'write.parquet.compression-codec'='snappy',
        'classification'='parquet',
        'format-version'='2'
    )
    """)


    spark.sql(f"""ALTER TABLE {table_name} WRITE ORDERED BY adapterTimestamp_ts_utc""") # Comment out this if you want to create unsorted data

    spark.sql(f"INSERT INTO {table_name} SELECT * FROM {temp_view}").collect()
