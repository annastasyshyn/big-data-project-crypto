import os
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    DoubleType,
)

KAFKA_BROKER = os.getenv("KAFKA_BROKER", "kafka:9092")
CASSANDRA_HOST = os.getenv("CASSANDRA_HOST", "cassandra")
KEYSPACE = os.getenv("CASSANDRA_KEYSPACE", "crypto")

spark = (
    SparkSession.builder.appName("MarketMomentum")
    .config("spark.cassandra.connection.host", CASSANDRA_HOST)
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

trade_schema = StructType(
    [
        StructField("timestamp", StringType()),
        StructField("symbol", StringType()),
        StructField("price", DoubleType()),
        StructField("size", IntegerType()),
        StructField("side", StringType()),
        StructField("trade_id", StringType()),
    ]
)

raw = (
    spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_BROKER)
    .option("subscribe", "raw-trades")
    .option("startingOffsets", "latest")
    .load()
)

trades = (
    raw.select(F.from_json(F.col("value").cast("string"), trade_schema).alias("d"))
    .select("d.*")
    .withColumn("trade_time", F.to_timestamp("timestamp"))
    .withColumn("volume_usd", F.col("size") * F.col("price"))
    .withColumn("is_buy", (F.col("side") == "Buy").cast("int"))
)

momentum = (
    trades.withWatermark("trade_time", "2 minutes")
    .groupBy(F.window("trade_time", "1 minute"), F.col("symbol"))
    .agg(
        F.last("price").alias("last_price"),
        F.first("price").alias("first_price"),
        F.sum("volume_usd").alias("volume_usd"),
        (F.sum("is_buy") / F.count("*")).alias("buy_ratio"),
        F.count("*").alias("trade_count"),
    )
    .select(
        "symbol",
        F.col("window.start").alias("window_start"),
        "last_price",
        F.round(
            (F.col("last_price") - F.col("first_price")) / F.col("first_price") * 100, 4
        ).alias("price_change_pct"),
        "volume_usd",
        F.round("buy_ratio", 4).alias("buy_ratio"),
    )
)


def write_to_cassandra(batch_df, batch_id):
    (
        batch_df.write.format("org.apache.spark.sql.cassandra")
        .mode("append")
        .options(table="market_momentum", keyspace=KEYSPACE)
        .save()
    )


query = (
    momentum.writeStream.foreachBatch(write_to_cassandra)
    .option("checkpointLocation", "/tmp/checkpoints/market-momentum")
    .outputMode("update")
    .start()
)

query.awaitTermination()
