import logging
from contextlib import asynccontextmanager

from dotenv import load_dotenv
from fastapi import FastAPI

from db import connect, get_session, shutdown
from routers import analytics, prices, reports, trades

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
log = logging.getLogger("api")


@asynccontextmanager
async def lifespan(app: FastAPI):
    log.info("API starting; connecting to Cassandra...")
    connect()
    yield
    log.info("API shutting down")
    shutdown()


app = FastAPI(
    title="Crypto Analytics API",
    version="1.0.0",
    description="Batch + ad-hoc analytics over Bitmex trade data stored in Cassandra.",
    lifespan=lifespan,
)

app.include_router(reports.router, prefix="/api", tags=["reports"])
app.include_router(analytics.router, prefix="/api", tags=["analytics"])
app.include_router(prices.router, prefix="/api", tags=["prices"])
app.include_router(trades.router, prefix="/api", tags=["trades"])


@app.get("/health", tags=["meta"])
def health() -> dict:
    return {"status": "ok"}


@app.get("/ready", tags=["meta"])
def ready() -> dict:
    session = get_session()
    row = session.execute("SELECT release_version FROM system.local").one()
    return {"status": "ready", "cassandra_version": row.release_version if row else None}
