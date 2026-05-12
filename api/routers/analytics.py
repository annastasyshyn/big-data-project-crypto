import re
import statistics
from datetime import datetime, timedelta, timezone
from typing import List, Optional

from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel, Field

from db import get_session

router = APIRouter()


# ---------------------------------------------------------------------------
# B2: GET /api/analytics/trading-patterns
# ---------------------------------------------------------------------------

SELECT_PATTERNS = """
SELECT hour_of_day, avg_trades, avg_volume, avg_spread, avg_volatility
FROM trading_patterns
WHERE symbol=%s
"""


class HourPattern(BaseModel):
    hour_of_day: int
    avg_trades: float
    avg_volume: float
    avg_spread: float
    avg_volatility: float


class TradingPatternsResponse(BaseModel):
    symbol: str
    bucket_count: int
    hours: List[HourPattern] = Field(default_factory=list)
    most_active_hours: List[int] = Field(default_factory=list)
    most_volatile_hours: List[int] = Field(default_factory=list)
    widest_spread_hours: List[int] = Field(default_factory=list)


def _top_hours(items: List[HourPattern], key, n: int = 3) -> List[int]:
    ranked = sorted(items, key=key, reverse=True)
    return [p.hour_of_day for p in ranked[:n]]


@router.get(
    "/analytics/trading-patterns",
    response_model=TradingPatternsResponse,
    summary="Trading patterns by hour-of-day (B2)",
)
def trading_patterns(
    symbol: str = Query(..., min_length=1, max_length=20, description="Trading symbol, e.g. XBTUSD"),
    top_n: int = Query(3, ge=1, le=24, description="How many top hours per metric to return"),
) -> TradingPatternsResponse:
    symbol = symbol.upper().strip()
    session = get_session()
    rows = list(session.execute(SELECT_PATTERNS, (symbol,)))

    if not rows:
        return TradingPatternsResponse(symbol=symbol, bucket_count=0)

    hours: List[HourPattern] = [
        HourPattern(
            hour_of_day=int(r.hour_of_day),
            avg_trades=float(r.avg_trades or 0.0),
            avg_volume=float(r.avg_volume or 0.0),
            avg_spread=float(r.avg_spread or 0.0),
            avg_volatility=float(r.avg_volatility or 0.0),
        )
        for r in rows
    ]
    hours.sort(key=lambda p: p.hour_of_day)

    return TradingPatternsResponse(
        symbol=symbol,
        bucket_count=len(hours),
        hours=hours,
        most_active_hours=_top_hours(hours, lambda p: p.avg_trades, top_n),
        most_volatile_hours=_top_hours(hours, lambda p: p.avg_volatility, top_n),
        widest_spread_hours=_top_hours(hours, lambda p: p.avg_spread, top_n),
    )


# ---------------------------------------------------------------------------
# B3: GET /api/analytics/whale-impact
# ---------------------------------------------------------------------------

SELECT_WHALES = """
SELECT trade_time, trade_id, trade_size, trade_price, side,
       avg_price_before, avg_price_after, price_impact_pct
FROM whale_impact
WHERE symbol=%s AND trade_time >= %s AND trade_time < %s
"""

_PERIOD_RE = re.compile(r"^(\d+)([hd])$", re.IGNORECASE)
_MAX_PERIOD_HOURS = 24 * 30  # 30 days


def _parse_period_to_hours(period: str) -> int:
    match = _PERIOD_RE.match(period.strip())
    if not match:
        raise HTTPException(
            status_code=422,
            detail="period must look like '1h', '24h', '7d' (h or d).",
        )
    value = int(match.group(1))
    unit = match.group(2).lower()
    hours = value if unit == "h" else value * 24
    if hours <= 0 or hours > _MAX_PERIOD_HOURS:
        raise HTTPException(
            status_code=422,
            detail=f"period must resolve to 1..{_MAX_PERIOD_HOURS} hours (max 30d).",
        )
    return hours


class WhaleTrade(BaseModel):
    trade_time: datetime
    trade_id: str
    trade_size: int
    trade_price: float
    side: str
    avg_price_before: float
    avg_price_after: float
    price_impact_pct: float


class WhaleImpactSummary(BaseModel):
    whale_count: int
    avg_impact_pct: float
    avg_abs_impact_pct: float
    median_impact_pct: float
    max_positive_impact_pct: float
    max_negative_impact_pct: float
    buy_whales: int
    sell_whales: int
    avg_impact_buy_pct: float
    avg_impact_sell_pct: float
    total_whale_volume_usd: float


class WhaleImpactResponse(BaseModel):
    symbol: str
    period: str
    period_start: datetime
    period_end: datetime
    summary: Optional[WhaleImpactSummary] = None
    top_whales: List[WhaleTrade] = Field(default_factory=list)


def _as_utc(dt: datetime) -> datetime:
    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def _round(value: float, ndigits: int = 4) -> float:
    return round(float(value), ndigits)


@router.get(
    "/analytics/whale-impact",
    response_model=WhaleImpactResponse,
    summary="Whale trades + price impact analysis (B3)",
)
def whale_impact(
    symbol: str = Query(..., min_length=1, max_length=20, description="Trading symbol, e.g. XBTUSD"),
    period: str = Query("24h", description="Look-back period, e.g. '1h', '24h', '7d', '30d'"),
    top_n: int = Query(20, ge=1, le=200, description="How many top-impact whales to include"),
) -> WhaleImpactResponse:
    symbol = symbol.upper().strip()
    hours = _parse_period_to_hours(period)

    period_end = datetime.now(timezone.utc)
    period_start = period_end - timedelta(hours=hours)

    session = get_session()
    rows = list(session.execute(SELECT_WHALES, (symbol, period_start, period_end)))

    if not rows:
        return WhaleImpactResponse(
            symbol=symbol,
            period=period,
            period_start=period_start,
            period_end=period_end,
        )

    whales: List[WhaleTrade] = [
        WhaleTrade(
            trade_time=_as_utc(r.trade_time),
            trade_id=r.trade_id,
            trade_size=int(r.trade_size or 0),
            trade_price=float(r.trade_price or 0.0),
            side=r.side or "Unknown",
            avg_price_before=float(r.avg_price_before or 0.0),
            avg_price_after=float(r.avg_price_after or 0.0),
            price_impact_pct=float(r.price_impact_pct or 0.0),
        )
        for r in rows
    ]

    impacts = [w.price_impact_pct for w in whales]
    buy_whales = [w for w in whales if w.side == "Buy"]
    sell_whales = [w for w in whales if w.side == "Sell"]
    buy_impacts = [w.price_impact_pct for w in buy_whales]
    sell_impacts = [w.price_impact_pct for w in sell_whales]

    summary = WhaleImpactSummary(
        whale_count=len(whales),
        avg_impact_pct=_round(statistics.fmean(impacts)),
        avg_abs_impact_pct=_round(statistics.fmean(abs(x) for x in impacts)),
        median_impact_pct=_round(statistics.median(impacts)),
        max_positive_impact_pct=_round(max(impacts)),
        max_negative_impact_pct=_round(min(impacts)),
        buy_whales=len(buy_whales),
        sell_whales=len(sell_whales),
        avg_impact_buy_pct=_round(statistics.fmean(buy_impacts)) if buy_impacts else 0.0,
        avg_impact_sell_pct=_round(statistics.fmean(sell_impacts)) if sell_impacts else 0.0,
        total_whale_volume_usd=_round(
            sum(w.trade_size * w.trade_price for w in whales), 2
        ),
    )

    top_whales = sorted(whales, key=lambda w: abs(w.price_impact_pct), reverse=True)[:top_n]

    return WhaleImpactResponse(
        symbol=symbol,
        period=period,
        period_start=period_start,
        period_end=period_end,
        summary=summary,
        top_whales=top_whales,
    )
