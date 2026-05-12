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

spark = (
    SparkSession.builder.appName("WhaleAlert")
    .config("spark.cassandra.connection.host", os.getenv("CASSANDRA_HOST", "cassandra"))
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

trade_schema = StructType(
    [
        StructField("timestamp", StringType()),
        StructField("symbol", StringType()),
        StructField("side", StringType()),
        StructField("size", IntegerType()),
        StructField("price", DoubleType()),
        StructField("trade_id", StringType()),
    ]
)

raw = (
    spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_BROKER)
    .option("subscribe", "raw-trades")
    .option("startingOffsets", "latest")
    .option("failOnDataLoss", "false")
    .load()
)

trades = (
    raw.select(F.from_json(F.col("value").cast("string"), trade_schema).alias("d"))
    .select("d.*")
    .withColumn("trade_time", F.to_timestamp("timestamp"))
    .withColumn("volume_usd", F.col("size") * F.col("price"))
)

windowed = (
    trades.withWatermark("trade_time", "10 minutes")
    .groupBy(
        F.window("trade_time", "10 minutes", "1 minute"),
        F.col("symbol"),
    )
    .agg(
        F.percentile_approx("size", 0.95).alias("threshold_95p"),
        F.collect_list(
            F.struct("trade_time", "size", "price", "side", "trade_id")
        ).alias("window_trades"),
    )
)

exploded = (
    windowed.select("symbol", "threshold_95p", F.explode("window_trades").alias("t"))
    .select(
        "symbol",
        "threshold_95p",
        F.col("t.trade_time").alias("alert_time"),
        F.col("t.size").alias("trade_size"),
        F.col("t.price").alias("price"),
        F.col("t.side").alias("side"),
        F.col("t.trade_id").alias("trade_id"),
    )
    .filter(F.col("trade_size") > F.col("threshold_95p"))
    .withColumn(
        "deviation_percent",
        F.round(
            (F.col("trade_size") - F.col("threshold_95p"))
            / F.col("threshold_95p")
            * 100,
            1,
        ),
    )
)

alert_df = exploded.select(
    F.col("symbol").alias("key"),
    F.to_json(
        F.struct(
            F.date_format("alert_time", "yyyy-MM-dd HH:mm:ss").alias("alert_time"),
            "symbol",
            "trade_size",
            "threshold_95p",
            "price",
            "side",
            "deviation_percent",
        )
    ).alias("value"),
)

query = (
    alert_df.writeStream.format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_BROKER)
    .option("topic", "whale-alerts")
    .option("checkpointLocation", "/tmp/checkpoints/whale-alert")
    .outputMode("append")
    .start()
)

query.awaitTermination()
