from datetime import datetime, timezone
from typing import List, Literal, Optional

from fastapi import APIRouter, Query
from pydantic import BaseModel, Field

from db import get_session

router = APIRouter()

SELECT_TRADES = """
SELECT symbol, trade_time, trade_id, price, size, side
FROM trades
WHERE symbol=%s
LIMIT %s
"""

Side = Literal["Buy", "Sell"]
MAX_LIMIT = 1000
MAX_SCAN_ROWS = 10000


class TradeItem(BaseModel):
    trade_time: datetime
    symbol: str
    trade_id: str
    price: float
    size: int
    side: str


class TradesResponse(BaseModel):
    symbol: str
    min_size: Optional[int] = None
    side: Optional[Side] = None
    limit: int
    returned: int
    trades: List[TradeItem] = Field(default_factory=list)


def _as_utc(dt: datetime) -> datetime:
    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


@router.get(
    "/trades",
    response_model=TradesResponse,
    summary="Trade lookup with filters (C2)",
)
def get_trades(
    symbol: str = Query(..., min_length=1, max_length=20, description="Trading symbol, e.g. XBTUSD"),
    min_size: Optional[int] = Query(None, ge=1, description="Minimum trade size filter"),
    side: Optional[Side] = Query(None, description="Trade side filter"),
    limit: int = Query(100, ge=1, le=MAX_LIMIT, description="Maximum number of records to return"),
) -> TradesResponse:
    symbol = symbol.upper().strip()

    scan_limit = min(MAX_SCAN_ROWS, max(limit * 10, limit))
    session = get_session()
    rows = list(session.execute(SELECT_TRADES, (symbol, scan_limit)))

    trades: List[TradeItem] = []
    for row in rows:
        row_side = (row.side or "").strip()

        if min_size is not None and int(row.size or 0) < min_size:
            continue
        if side is not None and row_side != side:
            continue

        trades.append(
            TradeItem(
                trade_time=_as_utc(row.trade_time),
                symbol=row.symbol,
                trade_id=row.trade_id,
                price=float(row.price or 0.0),
                size=int(row.size or 0),
                side=row_side or "Unknown",
            )
        )
        if len(trades) >= limit:
            break

    return TradesResponse(
        symbol=symbol,
        min_size=min_size,
        side=side,
        limit=limit,
        returned=len(trades),
        trades=trades,
    )
