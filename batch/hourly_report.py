import os
import logging
from datetime import datetime, timedelta, timezone

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
log = logging.getLogger("hourly_report")

CASSANDRA_HOST = os.getenv("CASSANDRA_HOST", "cassandra")
KEYSPACE = os.getenv("CASSANDRA_KEYSPACE", "crypto")
HOURS_BACK = int(os.getenv("HOURLY_REPORT_HOURS_BACK", "12"))


def build_spark() -> SparkSession:
    return (
        SparkSession.builder.appName("HourlyReport")
        .config("spark.cassandra.connection.host", CASSANDRA_HOST)
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )


def main() -> None:
    spark = build_spark()
    spark.sparkContext.setLogLevel("WARN")

    now_utc = datetime.now(timezone.utc).replace(minute=0, second=0, microsecond=0)
    end_time = now_utc
    start_time = now_utc - timedelta(hours=HOURS_BACK)
    log.info(
        "Computing hourly report for %s <= trade_time < %s (%d hours)",
        start_time.isoformat(),
        end_time.isoformat(),
        HOURS_BACK,
    )

    trades = (
        spark.read.format("org.apache.spark.sql.cassandra")
        .options(table="trades", keyspace=KEYSPACE)
        .load()
        .filter(
            (F.col("trade_time") >= F.lit(start_time))
            & (F.col("trade_time") < F.lit(end_time))
        )
        .filter(F.col("price").isNotNull() & F.col("size").isNotNull())
    )

    enriched = trades.withColumn(
        "hour_bucket", F.date_trunc("hour", F.col("trade_time"))
    ).withColumn("volume_usd", F.col("size") * F.col("price"))

    agg = enriched.groupBy("symbol", "hour_bucket").agg(
        F.count(F.lit(1)).alias("trade_count"),
        F.sum("volume_usd").alias("total_volume"),
        F.min("price").alias("min_price"),
        F.max("price").alias("max_price"),
        F.avg("price").alias("avg_price"),
        F.stddev("price").alias("volatility"),
        F.sum(
            F.when(F.col("side") == "Buy", F.col("volume_usd")).otherwise(F.lit(0.0))
        ).alias("buy_volume"),
        F.sum(
            F.when(F.col("side") == "Sell", F.col("volume_usd")).otherwise(F.lit(0.0))
        ).alias("sell_volume"),
    )

    result = (
        agg.withColumn(
            "dominant_side",
            F.when(F.col("buy_volume") >= F.col("sell_volume"), F.lit("Buy")).otherwise(
                F.lit("Sell")
            ),
        )
        .withColumn("volatility", F.coalesce(F.col("volatility"), F.lit(0.0)))
        .withColumn("trade_count", F.col("trade_count").cast("int"))
        .select(
            "symbol",
            "hour_bucket",
            "trade_count",
            F.col("total_volume").cast("double"),
            F.col("min_price").cast("double"),
            F.col("max_price").cast("double"),
            F.col("avg_price").cast("double"),
            F.col("volatility").cast("double"),
            "dominant_side",
        )
    )

    rows = result.count()
    log.info("Producing %d (symbol, hour) rows for hourly_report", rows)

    if rows > 0:
        (
            result.write.format("org.apache.spark.sql.cassandra")
            .mode("append")
            .options(table="hourly_report", keyspace=KEYSPACE)
            .save()
        )
        log.info("Hourly report written to Cassandra")
    else:
        log.warning("No trades in window - nothing written")

    spark.stop()


if __name__ == "__main__":
    main()
