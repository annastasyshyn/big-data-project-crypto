import logging
import os
import subprocess
import sys
from datetime import datetime, timedelta, timezone

from apscheduler.schedulers.blocking import BlockingScheduler
from apscheduler.triggers.cron import CronTrigger

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
log = logging.getLogger("batch-scheduler")

SPARK_MASTER = os.getenv("SPARK_MASTER", "spark://spark-master:7077")
SPARK_SUBMIT = os.getenv("SPARK_SUBMIT", "/opt/spark/bin/spark-submit")
JOBS_DIR = os.getenv("BATCH_JOBS_DIR", "/opt/spark/jobs/batch")
PACKAGES = os.getenv(
    "SPARK_PACKAGES",
    "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,"
    "com.datastax.spark:spark-cassandra-connector_2.12:3.5.0",
)
CASSANDRA_HOST = os.getenv("CASSANDRA_HOST", "cassandra")
KEYSPACE = os.getenv("CASSANDRA_KEYSPACE", "crypto")
BATCH_JOB_CORES_MAX = int(os.getenv("BATCH_JOB_CORES_MAX", "1"))

JOB_TIMEOUT_SECONDS = int(os.getenv("JOB_TIMEOUT_SECONDS", "1800"))
RUN_ON_STARTUP = os.getenv("RUN_ON_STARTUP", "true").lower() in {"1", "true", "yes"}

HOURLY_CRON = os.getenv("HOURLY_REPORT_CRON", "5 * * * *")
PATTERNS_CRON = os.getenv("TRADING_PATTERNS_CRON", "*/30 * * * *")
WHALE_CRON = os.getenv("WHALE_IMPACT_CRON", "15 * * * *")


JOBS = {
    "hourly_report": {
        "script": "hourly_report.py",
        "cron": HOURLY_CRON,
        "executor_memory": "1g",
        "cores_max": BATCH_JOB_CORES_MAX,
    },
    "trading_patterns": {
        "script": "trading_patterns.py",
        "cron": PATTERNS_CRON,
        "executor_memory": "1g",
        "cores_max": BATCH_JOB_CORES_MAX,
    },
    "whale_impact": {
        "script": "whale_impact.py",
        "cron": WHALE_CRON,
        "executor_memory": "1g",
        "cores_max": BATCH_JOB_CORES_MAX,
    },
}


def submit_job(name: str) -> None:
    cfg = JOBS[name]
    script_path = os.path.join(JOBS_DIR, cfg["script"])

    cmd = [
        SPARK_SUBMIT,
        "--master", SPARK_MASTER,
        "--packages", PACKAGES,
        "--conf", "spark.jars.ivy=/tmp/.ivy2",
        "--conf", f"spark.cores.max={cfg['cores_max']}",
        "--conf", "spark.executor.cores=1",
        "--conf", f"spark.executor.memory={cfg['executor_memory']}",
        "--conf", f"spark.cassandra.connection.host={CASSANDRA_HOST}",
        script_path,
    ]

    env = {
        **os.environ,
        "CASSANDRA_HOST": CASSANDRA_HOST,
        "CASSANDRA_KEYSPACE": KEYSPACE,
    }

    log.info("[%s] submitting: %s", name, " ".join(cmd))
    started = datetime.now(timezone.utc)
    try:
        completed = subprocess.run(
            cmd,
            env=env,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            timeout=JOB_TIMEOUT_SECONDS,
            check=False,
        )
        duration = (datetime.now(timezone.utc) - started).total_seconds()
        tail = "\n".join(completed.stdout.splitlines()[-30:]) if completed.stdout else ""
        if completed.returncode == 0:
            log.info(
                "[%s] OK in %.1fs (rc=%d)\n--- last lines ---\n%s\n--- end ---",
                name, duration, completed.returncode, tail,
            )
        else:
            log.error(
                "[%s] FAILED in %.1fs (rc=%d)\n--- last lines ---\n%s\n--- end ---",
                name, duration, completed.returncode, tail,
            )
    except subprocess.TimeoutExpired:
        log.error("[%s] timed out after %ds", name, JOB_TIMEOUT_SECONDS)
    except Exception as exc:
        log.exception("[%s] crashed: %s", name, exc)


def main() -> None:
    log.info("Batch scheduler starting")
    log.info("Spark master: %s", SPARK_MASTER)
    log.info("Jobs dir: %s", JOBS_DIR)
    log.info("Cassandra host: %s keyspace: %s", CASSANDRA_HOST, KEYSPACE)

    scheduler = BlockingScheduler(timezone="UTC")

    for job_id, cfg in JOBS.items():
        trigger = CronTrigger.from_crontab(cfg["cron"], timezone="UTC")
        scheduler.add_job(
            submit_job,
            trigger=trigger,
            args=[job_id],
            id=job_id,
            name=job_id,
            max_instances=1,
            coalesce=True,
            misfire_grace_time=300,
        )
        log.info("Scheduled %s with cron '%s'", job_id, cfg["cron"])

    if RUN_ON_STARTUP:
        offset = 15
        for job_id in JOBS:
            when = datetime.now(timezone.utc) + timedelta(seconds=offset)
            scheduler.add_job(
                submit_job,
                trigger="date",
                run_date=when,
                args=[job_id],
                id=f"{job_id}-startup",
                name=f"{job_id}-startup",
                max_instances=1,
                coalesce=True,
                misfire_grace_time=600,
            )
            log.info("Startup-run for %s queued at %s", job_id, when.isoformat())
            offset += 30

    try:
        scheduler.start()
    except (KeyboardInterrupt, SystemExit):
        log.info("Scheduler stopping")
        sys.exit(0)


if __name__ == "__main__":
    main()
