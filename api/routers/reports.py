from datetime import datetime, timedelta, timezone
from typing import List, Optional

from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel, Field

from db import get_session

router = APIRouter()

SELECT_HOURLY = """
SELECT hour_bucket, trade_count, total_volume, min_price, max_price,
       avg_price, volatility, dominant_side
FROM hourly_report
WHERE symbol=%s AND hour_bucket >= %s AND hour_bucket < %s
"""


class HourlyBucket(BaseModel):
    hour_bucket: datetime
    trade_count: int
    total_volume: float
    min_price: float
    max_price: float
    avg_price: float
    volatility: float
    dominant_side: str


class HourlyTotals(BaseModel):
    trade_count: int
    total_volume_usd: float
    weighted_avg_price: float
    min_price: float
    max_price: float
    buy_dominant_hours: int
    sell_dominant_hours: int


class HourlyReportResponse(BaseModel):
    symbol: str
    hours_requested: int
    bucket_count: int
    period_start: datetime
    period_end: datetime
    totals: Optional[HourlyTotals] = None
    buckets: List[HourlyBucket] = Field(default_factory=list)


def _as_utc(dt: datetime) -> datetime:
    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


@router.get(
    "/reports/hourly",
    response_model=HourlyReportResponse,
    summary="Hourly trading report (B1)",
)
def hourly_report(
    symbol: str = Query(..., min_length=1, max_length=20, description="Trading symbol, e.g. XBTUSD"),
    hours: int = Query(12, ge=1, le=168, description="How many full hours back to include (1..168)"),
) -> HourlyReportResponse:
    symbol = symbol.upper().strip()

    now_top_of_hour = datetime.now(timezone.utc).replace(minute=0, second=0, microsecond=0)
    period_end = now_top_of_hour
    period_start = period_end - timedelta(hours=hours)

    session = get_session()
    rows = list(session.execute(SELECT_HOURLY, (symbol, period_start, period_end)))

    if not rows:
        return HourlyReportResponse(
            symbol=symbol,
            hours_requested=hours,
            bucket_count=0,
            period_start=period_start,
            period_end=period_end,
            totals=None,
            buckets=[],
        )

    buckets: List[HourlyBucket] = [
        HourlyBucket(
            hour_bucket=_as_utc(r.hour_bucket),
            trade_count=int(r.trade_count or 0),
            total_volume=float(r.total_volume or 0.0),
            min_price=float(r.min_price or 0.0),
            max_price=float(r.max_price or 0.0),
            avg_price=float(r.avg_price or 0.0),
            volatility=float(r.volatility or 0.0),
            dominant_side=r.dominant_side or "Unknown",
        )
        for r in rows
    ]
    buckets.sort(key=lambda b: b.hour_bucket, reverse=True)

    total_volume = sum(b.total_volume for b in buckets)
    total_count = sum(b.trade_count for b in buckets)
    weighted_avg_price = (
        sum(b.avg_price * b.total_volume for b in buckets) / total_volume
        if total_volume > 0
        else 0.0
    )
    buy_hours = sum(1 for b in buckets if b.dominant_side == "Buy")

    totals = HourlyTotals(
        trade_count=total_count,
        total_volume_usd=round(total_volume, 4),
        weighted_avg_price=round(weighted_avg_price, 4),
        min_price=min(b.min_price for b in buckets),
        max_price=max(b.max_price for b in buckets),
        buy_dominant_hours=buy_hours,
        sell_dominant_hours=len(buckets) - buy_hours,
    )

    return HourlyReportResponse(
        symbol=symbol,
        hours_requested=hours,
        bucket_count=len(buckets),
        period_start=period_start,
        period_end=period_end,
        totals=totals,
        buckets=buckets,
    )
