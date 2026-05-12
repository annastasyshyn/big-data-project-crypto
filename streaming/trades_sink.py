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
CHECKPOINT_LOCATION = os.getenv(
    "TRADES_SINK_CHECKPOINT",
    "/opt/spark/checkpoints/trades-sink",
)

spark = (
    SparkSession.builder.appName("TradesSink")
    .config("spark.cassandra.connection.host", CASSANDRA_HOST)
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
    raw.select(
        F.from_json(F.col("value").cast("string"), trade_schema).alias("d"),
        F.col("partition").alias("kafka_partition"),
        F.col("offset").alias("kafka_offset"),
    )
    .select("d.*", "kafka_partition", "kafka_offset")
    .withColumn("trade_time", F.to_timestamp("timestamp"))
    .withColumn(
        "trade_id",
        F.coalesce(
            F.when(F.length(F.trim(F.col("trade_id"))) > 0, F.col("trade_id")),
            F.concat(
                F.lit("kafka-"),
                F.col("kafka_partition").cast("string"),
                F.lit("-"),
                F.col("kafka_offset").cast("string"),
            ),
        ),
    )
    .filter(
        F.col("trade_time").isNotNull()
        & F.col("symbol").isNotNull()
    )
    .select(
        F.col("symbol"),
        F.col("trade_time"),
        F.col("trade_id"),
        F.col("price"),
        F.col("size"),
        F.col("side"),
    )
)


def write_batch(batch_df, batch_id):
    (
        batch_df.write.format("org.apache.spark.sql.cassandra")
        .mode("append")
        .options(table="trades", keyspace=KEYSPACE)
        .save()
    )


query = (
    trades.writeStream.foreachBatch(write_batch)
    .option("checkpointLocation", CHECKPOINT_LOCATION)
    .outputMode("append")
    .trigger(processingTime="10 seconds")
    .start()
)

query.awaitTermination()
