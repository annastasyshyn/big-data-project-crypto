from collections import defaultdict
from datetime import datetime, timezone
from typing import Dict, List, Literal

from fastapi import APIRouter, HTTPException, Path, Query
from pydantic import BaseModel, Field

from db import get_session

router = APIRouter()

SELECT_TRADES_IN_RANGE = """
SELECT trade_time, price, size
FROM trades
WHERE symbol=%s AND trade_time >= %s AND trade_time < %s
"""

Interval = Literal["1m", "5m", "1h"]
INTERVAL_SECONDS: Dict[Interval, int] = {"1m": 60, "5m": 300, "1h": 3600}


class Candle(BaseModel):
    time: datetime
    open: float
    high: float
    low: float
    close: float
    volume: float


class PriceHistoryResponse(BaseModel):
    symbol: str
    interval: Interval
    from_ts: datetime = Field(alias="from")
    to_ts: datetime = Field(alias="to")
    bucket_count: int
    candles: List[Candle] = Field(default_factory=list)

    model_config = {"populate_by_name": True}


def _as_utc(dt: datetime) -> datetime:
    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


@router.get(
    "/price/{symbol}",
    response_model=PriceHistoryResponse,
    summary="Price history with OHLCV candles (C1)",
)
def price_history(
    symbol: str = Path(..., min_length=1, max_length=20, description="Trading symbol, e.g. XBTUSD"),
    from_ts: datetime = Query(..., alias="from", description="Range start timestamp"),
    to_ts: datetime = Query(..., alias="to", description="Range end timestamp (exclusive)"),
    interval: Interval = Query("1m", description="Candle interval: 1m, 5m, or 1h"),
) -> PriceHistoryResponse:
    symbol = symbol.upper().strip()
    from_ts = _as_utc(from_ts)
    to_ts = _as_utc(to_ts)

    if from_ts >= to_ts:
        raise HTTPException(status_code=422, detail="'from' must be earlier than 'to'.")

    session = get_session()
    rows = list(session.execute(SELECT_TRADES_IN_RANGE, (symbol, from_ts, to_ts)))
    if not rows:
        return PriceHistoryResponse(
            symbol=symbol,
            interval=interval,
            from_ts=from_ts,
            to_ts=to_ts,
            bucket_count=0,
            candles=[],
        )

    bucket_sec = INTERVAL_SECONDS[interval]
    buckets: Dict[int, Dict[str, object]] = defaultdict(dict)

    sorted_rows = sorted(rows, key=lambda r: _as_utc(r.trade_time))
    for row in sorted_rows:
        trade_ts = _as_utc(row.trade_time)
        bucket_epoch = int(trade_ts.timestamp() // bucket_sec) * bucket_sec
        bucket = buckets[bucket_epoch]
        price = float(row.price or 0.0)
        size = float(row.size or 0.0)

        if "open" not in bucket:
            bucket["open"] = price
            bucket["high"] = price
            bucket["low"] = price
            bucket["volume"] = 0.0

        bucket["high"] = max(float(bucket["high"]), price)
        bucket["low"] = min(float(bucket["low"]), price)
        bucket["close"] = price
        bucket["volume"] = float(bucket["volume"]) + size

    candles = [
        Candle(
            time=datetime.fromtimestamp(epoch, tz=timezone.utc),
            open=float(data["open"]),
            high=float(data["high"]),
            low=float(data["low"]),
            close=float(data["close"]),
            volume=float(data["volume"]),
        )
        for epoch, data in sorted(buckets.items())
    ]

    return PriceHistoryResponse(
        symbol=symbol,
        interval=interval,
        from_ts=from_ts,
        to_ts=to_ts,
        bucket_count=len(candles),
        candles=candles,
    )
