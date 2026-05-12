import os
import logging
from datetime import datetime, timedelta, timezone

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
log = logging.getLogger("whale_impact")

CASSANDRA_HOST = os.getenv("CASSANDRA_HOST", "cassandra")
KEYSPACE = os.getenv("CASSANDRA_KEYSPACE", "crypto")
HOURS_BACK = int(os.getenv("WHALE_IMPACT_HOURS", "24"))
IMPACT_WINDOW_SECONDS = int(os.getenv("WHALE_IMPACT_WINDOW_SECONDS", "300"))
PERCENTILE = float(os.getenv("WHALE_IMPACT_PERCENTILE", "0.90"))


def build_spark() -> SparkSession:
    return (
        SparkSession.builder.appName("WhaleImpact")
        .config("spark.cassandra.connection.host", CASSANDRA_HOST)
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )


def main() -> None:
    spark = build_spark()
    spark.sparkContext.setLogLevel("WARN")

    end_time = datetime.now(timezone.utc)
    start_time = end_time - timedelta(hours=HOURS_BACK)
    log.info(
        "Analysing whale impact for %s <= trade_time < %s (%d hours, p%.0f, +/-%ds)",
        start_time.isoformat(),
        end_time.isoformat(),
        HOURS_BACK,
        PERCENTILE * 100,
        IMPACT_WINDOW_SECONDS,
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
        .withColumn("unix_ts", F.col("trade_time").cast("long"))
    ).cache()

    if trades.rdd.isEmpty():
        log.warning("No trades in window - nothing written")
        spark.stop()
        return

    thresholds = trades.groupBy("symbol").agg(
        F.expr(f"percentile_approx(size, {PERCENTILE})").alias("p_threshold")
    )

    before_w = (
        Window.partitionBy("symbol")
        .orderBy("unix_ts")
        .rangeBetween(-IMPACT_WINDOW_SECONDS, -1)
    )
    after_w = (
        Window.partitionBy("symbol")
        .orderBy("unix_ts")
        .rangeBetween(1, IMPACT_WINDOW_SECONDS)
    )

    enriched = (
        trades.withColumn("avg_price_before", F.avg("price").over(before_w))
        .withColumn("avg_price_after", F.avg("price").over(after_w))
        .join(F.broadcast(thresholds), "symbol", "inner")
    )

    whales = (
        enriched.filter(F.col("size") > F.col("p_threshold"))
        .filter(
            F.col("avg_price_before").isNotNull()
            & F.col("avg_price_after").isNotNull()
            & (F.col("avg_price_before") > 0)
        )
        .withColumn(
            "price_impact_pct",
            F.round(
                (F.col("avg_price_after") - F.col("avg_price_before"))
                / F.col("avg_price_before")
                * 100.0,
                4,
            ),
        )
        .select(
            "symbol",
            "trade_time",
            "trade_id",
            F.col("size").cast("int").alias("trade_size"),
            F.col("price").cast("double").alias("trade_price"),
            "side",
            F.col("avg_price_before").cast("double"),
            F.col("avg_price_after").cast("double"),
            F.col("price_impact_pct").cast("double"),
        )
    ).cache()

    whale_count = whales.count()
    log.info("Found %d whale trades to upsert into whale_impact", whale_count)

    if whale_count > 0:
        (
            whales.write.format("org.apache.spark.sql.cassandra")
            .mode("append")
            .options(table="whale_impact", keyspace=KEYSPACE)
            .save()
        )

        summary = whales.groupBy("symbol").agg(
            F.count(F.lit(1)).alias("whales"),
            F.avg("price_impact_pct").alias("avg_impact_pct"),
            F.avg(F.abs(F.col("price_impact_pct"))).alias("avg_abs_impact_pct"),
        )
        for row in summary.collect():
            log.info(
                "[whale-summary] symbol=%s whales=%d avg_impact=%.4f%% avg_abs_impact=%.4f%%",
                row["symbol"],
                row["whales"],
                row["avg_impact_pct"] or 0.0,
                row["avg_abs_impact_pct"] or 0.0,
            )

    trades.unpersist()
    whales.unpersist()
    spark.stop()


if __name__ == "__main__":
    main()
