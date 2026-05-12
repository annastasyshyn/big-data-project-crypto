CREATE KEYSPACE IF NOT EXISTS crypto
  WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};

USE crypto;

CREATE TABLE IF NOT EXISTS trades (
    symbol     TEXT,
    trade_time TIMESTAMP,
    trade_id   TEXT,
    price      DOUBLE,
    size       INT,
    side       TEXT,
    PRIMARY KEY ((symbol), trade_time, trade_id)
) WITH CLUSTERING ORDER BY (trade_time DESC)
  AND default_time_to_live = 604800;

CREATE TABLE IF NOT EXISTS market_momentum (
    symbol           TEXT,
    window_start     TIMESTAMP,
    last_price       DOUBLE,
    price_change_pct DOUBLE,
    volume_usd       DOUBLE,
    buy_ratio        DOUBLE,
    PRIMARY KEY (symbol, window_start)
) WITH CLUSTERING ORDER BY (window_start DESC);

CREATE TABLE IF NOT EXISTS hourly_report (
    symbol        TEXT,
    hour_bucket   TIMESTAMP,
    trade_count   INT,
    total_volume  DOUBLE,
    min_price     DOUBLE,
    max_price     DOUBLE,
    avg_price     DOUBLE,
    volatility    DOUBLE,
    dominant_side TEXT,
    PRIMARY KEY ((symbol), hour_bucket)
) WITH CLUSTERING ORDER BY (hour_bucket DESC);

CREATE TABLE IF NOT EXISTS trading_patterns (
    symbol         TEXT,
    hour_of_day    INT,
    avg_trades     DOUBLE,
    avg_volume     DOUBLE,
    avg_spread     DOUBLE,
    avg_volatility DOUBLE,
    PRIMARY KEY (symbol, hour_of_day)
);

CREATE TABLE IF NOT EXISTS whale_impact (
    symbol           TEXT,
    trade_time       TIMESTAMP,
    trade_id         TEXT,
    trade_size       INT,
    trade_price      DOUBLE,
    side             TEXT,
    avg_price_before DOUBLE,
    avg_price_after  DOUBLE,
    price_impact_pct DOUBLE,
    PRIMARY KEY ((symbol), trade_time, trade_id)
) WITH CLUSTERING ORDER BY (trade_time DESC);
