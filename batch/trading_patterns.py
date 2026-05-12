import os
import logging

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
log = logging.getLogger("trading_patterns")

CASSANDRA_HOST = os.getenv("CASSANDRA_HOST", "cassandra")
KEYSPACE = os.getenv("CASSANDRA_KEYSPACE", "crypto")


def build_spark() -> SparkSession:
    return (
        SparkSession.builder.appName("TradingPatterns")
        .config("spark.cassandra.connection.host", CASSANDRA_HOST)
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )


def main() -> None:
    spark = build_spark()
    spark.sparkContext.setLogLevel("WARN")

    trades = (
        spark.read.format("org.apache.spark.sql.cassandra")
        .options(table="trades", keyspace=KEYSPACE)
        .load()
        .filter(F.col("price").isNotNull() & F.col("size").isNotNull())
        .withColumn("volume_usd", F.col("size") * F.col("price"))
        .withColumn("hour_bucket", F.date_trunc("hour", F.col("trade_time")))
        .withColumn("hour_of_day", F.hour("trade_time"))
    )

    per_bucket = trades.groupBy("symbol", "hour_of_day", "hour_bucket").agg(
        F.count(F.lit(1)).alias("trade_count"),
        F.sum("volume_usd").alias("volume_usd"),
        (F.max("price") - F.min("price")).alias("spread"),
        F.stddev("price").alias("volatility"),
    )

    patterns = (
        per_bucket.groupBy("symbol", "hour_of_day")
        .agg(
            F.avg("trade_count").alias("avg_trades"),
            F.avg("volume_usd").alias("avg_volume"),
            F.avg("spread").alias("avg_spread"),
            F.avg("volatility").alias("avg_volatility"),
        )
        .withColumn("avg_volatility", F.coalesce(F.col("avg_volatility"), F.lit(0.0)))
        .withColumn("avg_spread", F.coalesce(F.col("avg_spread"), F.lit(0.0)))
        .select(
            "symbol",
            F.col("hour_of_day").cast("int").alias("hour_of_day"),
            F.col("avg_trades").cast("double"),
            F.col("avg_volume").cast("double"),
            F.col("avg_spread").cast("double"),
            F.col("avg_volatility").cast("double"),
        )
    )

    rows = patterns.count()
    log.info("Producing %d (symbol, hour_of_day) rows for trading_patterns", rows)

    if rows > 0:
        (
            patterns.write.format("org.apache.spark.sql.cassandra")
            .mode("append")
            .options(table="trading_patterns", keyspace=KEYSPACE)
            .save()
        )
        log.info("Trading patterns written to Cassandra")
    else:
        log.warning("No trades found - nothing written")

    spark.stop()


if __name__ == "__main__":
    main()
