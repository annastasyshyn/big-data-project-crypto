import asyncio
import json
import logging
import os
import signal
import time
import websockets
from kafka import KafkaProducer
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
log = logging.getLogger(__name__)

BITMEX_WS_URL = os.getenv("BITMEX_WS_URL", "wss://ws.bitmex.com/realtime")
KAFKA_BROKER = os.getenv("KAFKA_BROKER", "localhost:9092")
SYMBOLS = os.getenv("SYMBOLS", "XBTUSD,ETHUSD").split(",")
TOPIC = "raw-trades"


def create_producer() -> KafkaProducer:
    for attempt in range(10):
        try:
            producer = KafkaProducer(
                bootstrap_servers=KAFKA_BROKER,
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                key_serializer=lambda k: k.encode("utf-8"),
                acks="all",
                retries=5,
                linger_ms=10,
                compression_type="gzip",
            )
            log.info(f"Kafka producer connected to {KAFKA_BROKER}")
            return producer
        except Exception as e:
            log.warning(f"Kafka connect attempt {attempt+1}/10 failed: {e}")
            time.sleep(5)
    raise RuntimeError("Cannot connect to Kafka after 10 attempts")


def handle_send_error(exc):
    log.error(f"Kafka send failed: {exc}")


def handle_signal(sig, shutdown_event: asyncio.Event) -> None:
    log.info(f"Shutdown signal received: {sig}")
    shutdown_event.set()


def install_signal_handlers(shutdown_event: asyncio.Event) -> None:
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGTERM, signal.SIGINT):
        loop.add_signal_handler(sig, handle_signal, sig, shutdown_event)


async def ingest(shutdown_event: asyncio.Event) -> None:
    producer = create_producer()
    log.info(f"Subscribing to symbols: {SYMBOLS}")

    subscribe_msg = {
        "op": "subscribe",
        "args": [f"trade:{s}" for s in SYMBOLS],
    }

    sent_total = 0

    try:
        while not shutdown_event.is_set():
            try:
                async with websockets.connect(
                    BITMEX_WS_URL,
                    ping_interval=20,
                    ping_timeout=30,
                    close_timeout=10,
                ) as ws:
                    await ws.send(json.dumps(subscribe_msg))
                    log.info("WebSocket connected, subscription sent")

                    async for raw in ws:
                        if shutdown_event.is_set():
                            break

                        msg = json.loads(raw)

                        if "subscribe" in msg:
                            log.info(f"Subscription ack: {msg}")
                            continue

                        if msg.get("table") != "trade" or "data" not in msg:
                            continue

                        for trade in msg["data"]:
                            record = {
                                "timestamp": trade.get("timestamp"),
                                "symbol": trade.get("symbol"),
                                "side": trade.get("side"),
                                "size": trade.get("size"),
                                "price": trade.get("price"),
                                "trade_id": trade.get("trdMatchID"),
                            }

                            if (
                                record["timestamp"] is None
                                or record["symbol"] is None
                                or record["size"] is None
                                or record["price"] is None
                            ):
                                log.warning(f"Skipping incomplete trade: {record}")
                                continue

                            future = producer.send(
                                TOPIC,
                                key=record["symbol"],
                                value=record,
                            )
                            future.add_errback(handle_send_error)
                            sent_total += 1

                        producer.flush()

                        if sent_total % 500 == 0:
                            log.info(f"Total trades sent to Kafka: {sent_total}")

            except websockets.ConnectionClosed as e:
                log.warning(f"WebSocket closed: {e}. Reconnecting in 5s...")
                await asyncio.sleep(5)
            except Exception as e:
                log.error(f"Unexpected error: {e}. Reconnecting in 10s...")
                await asyncio.sleep(10)
    finally:
        log.info(f"Shutting down. Total trades sent: {sent_total}")
        producer.flush()
        producer.close()


async def main() -> None:
    shutdown_event = asyncio.Event()
    install_signal_handlers(shutdown_event)
    await ingest(shutdown_event)


if __name__ == "__main__":
    asyncio.run(main())
