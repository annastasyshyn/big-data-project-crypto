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
CHECKPOINT_LOCATION = os.getenv(
    "VOLATILITY_ALERT_CHECKPOINT", "/tmp/checkpoints/volatility-alerts"
)

spark = SparkSession.builder.appName("VolatilityMonitor").getOrCreate()
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
)

volatility = (
    trades.withWatermark("trade_time", "15 minutes")
    .groupBy(F.window("trade_time", "5 minutes"), F.col("symbol"))
    .agg(
        F.stddev("price").alias("volatility"),
        F.count("*").alias("trade_count"),
    )
    .select(
        "symbol",
        F.col("window.start").alias("window_start"),
        F.col("window.end").alias("window_end"),
        "volatility",
        "trade_count",
    )
)

prev = volatility.alias("prev")
curr = volatility.alias("curr")

joined = curr.join(
    prev,
    (F.col("curr.symbol") == F.col("prev.symbol"))
    & (F.col("curr.window_start") == F.col("prev.window_end")),
    "inner",
).filter(F.col("curr.volatility") > 2 * F.col("prev.volatility"))

alert_df = joined.select(
    F.col("curr.symbol").alias("key"),
    F.to_json(
        F.struct(
            F.col("curr.symbol").alias("symbol"),
            F.date_format("curr.window_start", "yyyy-MM-dd HH:mm:ss").alias(
                "window_start"
            ),
            F.col("curr.volatility").alias("current_volatility"),
            F.col("prev.volatility").alias("prev_volatility"),
            F.round(F.col("curr.volatility") / F.col("prev.volatility"), 2).alias(
                "volatility_ratio"
            ),
        )
    ).alias("value"),
)

query = (
    alert_df.writeStream.format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_BROKER)
    .option("topic", "volatility-alerts")
    .option("checkpointLocation", CHECKPOINT_LOCATION)
    .outputMode("append")
    .start()
)

query.awaitTermination()