import logging
import os
import time
from typing import Optional

from cassandra.cluster import Cluster, Session

log = logging.getLogger(__name__)


_HOSTS_RAW = os.getenv("CASSANDRA_HOSTS") or os.getenv("CASSANDRA_HOST") or "cassandra"
CASSANDRA_HOSTS = [h.strip() for h in _HOSTS_RAW.split(",") if h.strip()]
CASSANDRA_PORT = int(os.getenv("CASSANDRA_PORT", "9042"))
CASSANDRA_KEYSPACE = os.getenv("CASSANDRA_KEYSPACE", "crypto")
CASSANDRA_CONNECT_TIMEOUT = int(os.getenv("CASSANDRA_CONNECT_TIMEOUT", "10"))
CASSANDRA_QUERY_TIMEOUT = float(os.getenv("CASSANDRA_QUERY_TIMEOUT", "15"))
CASSANDRA_CONNECT_RETRIES = int(os.getenv("CASSANDRA_CONNECT_RETRIES", "30"))
CASSANDRA_CONNECT_BACKOFF = float(os.getenv("CASSANDRA_CONNECT_BACKOFF", "5"))

_cluster: Optional[Cluster] = None
_session: Optional[Session] = None


def connect() -> Session:
    global _cluster, _session
    if _session is not None:
        return _session

    last_err: Optional[Exception] = None
    for attempt in range(1, CASSANDRA_CONNECT_RETRIES + 1):
        try:
            cluster = Cluster(
                contact_points=CASSANDRA_HOSTS,
                port=CASSANDRA_PORT,
                protocol_version=4,
                connect_timeout=CASSANDRA_CONNECT_TIMEOUT,
            )
            session = cluster.connect(CASSANDRA_KEYSPACE)
            session.default_timeout = CASSANDRA_QUERY_TIMEOUT
            _cluster, _session = cluster, session
            log.info(
                "Cassandra connected hosts=%s keyspace=%s",
                CASSANDRA_HOSTS,
                CASSANDRA_KEYSPACE,
            )
            return session
        except Exception as exc:
            last_err = exc
            log.warning(
                "Cassandra connect attempt %d/%d failed: %s",
                attempt,
                CASSANDRA_CONNECT_RETRIES,
                exc,
            )
            time.sleep(CASSANDRA_CONNECT_BACKOFF)

    raise RuntimeError(f"Cannot connect to Cassandra: {last_err}")


def get_session() -> Session:
    if _session is None:
        return connect()
    return _session


def shutdown() -> None:
    global _cluster, _session
    if _cluster is not None:
        try:
            _cluster.shutdown()
        except Exception as exc:
            log.warning("Cassandra shutdown error: %s", exc)
    _cluster = None
    _session = None
