"""Postgres backup flow — pg_dump to S3-compatible storage (R2/S3/B2).

Runs daily at 03:00 UTC. Backs up second_brain and prefect databases.
Sends Telegram notification on completion or failure.

Environment variables:
    BACKUP_S3_ENDPOINT_URL      — e.g. https://<id>.r2.cloudflarestorage.com (empty = AWS)
    BACKUP_S3_ACCESS_KEY_ID
    BACKUP_S3_SECRET_ACCESS_KEY
    BACKUP_S3_BUCKET            — e.g. backups
    BACKUP_S3_PREFIX            — optional prefix, default "postgres/"
    BACKUP_RETENTION_DAYS       — default 30
    DATABASE_URL                — postgresql://user:pass@host:port/db (for second_brain)
    PREFECT_POSTGRES_URL        — optional, for prefect DB (if different host)

Prefect Variables (UI-editable, no redeploy needed):
    backup_databases      = {"second_brain": "second_brain", "prefect": "prefect"}
    backup_retention_days = 30
"""
from __future__ import annotations

import gzip
import logging
import os
import shutil
import subprocess
import tempfile
from datetime import datetime, timezone
from typing import Any
from urllib.parse import urlparse

from prefect import flow, task
from prefect.variables import Variable

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Config helpers
# ---------------------------------------------------------------------------

def _get_r2_creds() -> dict:
    """Load R2 credentials from Prefect Secret block, falling back to env vars.

    Block name: ``s3-backup-credentials``
    Expected JSON shape::

        {
            "endpoint": "https://<id>.r2.cloudflarestorage.com",
            "access_key_id": "...",
            "secret_access_key": "..."
        }
    """
    import json

    try:
        from prefect.blocks.system import Secret
        raw = Secret.load("s3-backup-credentials").get()
        creds = json.loads(raw)
        return {
            "endpoint_url": creds.get("endpoint"),
            "access_key": creds.get("access_key_id"),
            "secret_key": creds.get("secret_access_key"),
        }
    except Exception:
        pass

    # Fall back to env vars (BACKUP_S3_* canonical, R2_* legacy)
    return {
        "endpoint_url": os.getenv("BACKUP_S3_ENDPOINT") or os.getenv("R2_ENDPOINT") or None,
        "access_key": os.getenv("BACKUP_S3_ACCESS_KEY_ID") or os.getenv("R2_ACCESS_KEY_ID"),
        "secret_key": os.getenv("BACKUP_S3_SECRET_ACCESS_KEY") or os.getenv("R2_SECRET_ACCESS_KEY"),
    }


def _get_s3_client():
    """Return a boto3 S3 client configured for S3-compatible storage."""
    import boto3

    creds = _get_r2_creds()
    if not creds["access_key"] or not creds["secret_key"]:
        return None

    return boto3.client(
        "s3",
        endpoint_url=creds["endpoint_url"],
        aws_access_key_id=creds["access_key"],
        aws_secret_access_key=creds["secret_key"],
        region_name="auto",  # R2 uses "auto"
    )


def _get_dsn(db_name: str) -> str:
    """Return connection DSN for a given database name."""
    base_url = os.getenv("DATABASE_URL", "")
    parsed = urlparse(base_url)
    # Replace the database component with the target db
    dsn = f"postgresql://{parsed.username}:{parsed.password}@{parsed.hostname}:{parsed.port or 5432}/{db_name}"
    return dsn


def _parse_dsn(dsn: str) -> dict[str, str]:
    p = urlparse(dsn)
    return {
        "host": p.hostname or "localhost",
        "port": str(p.port or 5432),
        "user": p.username or "",
        "password": p.password or "",
        "dbname": p.path.lstrip("/"),
    }


# ---------------------------------------------------------------------------
# Tasks
# ---------------------------------------------------------------------------

@task(name="dump-database", retries=2, retry_delay_seconds=30)
def dump_database(db_name: str, output_path: str) -> str:
    """Run pg_dump and compress output. Returns path to .sql.gz file."""
    dsn = _get_dsn(db_name)
    parts = _parse_dsn(dsn)

    gz_path = f"{output_path}/{db_name}_{datetime.now(timezone.utc).strftime('%Y-%m-%dT%H-%M')}.sql.gz"

    env = os.environ.copy()
    env["PGPASSWORD"] = parts["password"]

    cmd = [
        "pg_dump",
        "-h", parts["host"],
        "-p", parts["port"],
        "-U", parts["user"],
        "-d", parts["dbname"],
        "--no-password",
        "--format=plain",
    ]

    logger.info("Dumping %s...", db_name)
    result = subprocess.run(cmd, capture_output=True, env=env)

    if result.returncode != 0:
        raise RuntimeError(
            f"pg_dump failed for {db_name}: {result.stderr.decode()[:500]}"
        )

    with gzip.open(gz_path, "wb") as f:
        f.write(result.stdout)

    size_mb = os.path.getsize(gz_path) / 1024 / 1024
    logger.info("Dumped %s → %s (%.1f MB)", db_name, gz_path, size_mb)
    return gz_path


@task(name="upload-to-s3", retries=3, retry_delay_seconds=60)
def upload_to_s3(local_path: str, db_name: str) -> str | None:
    """Upload dump file to S3-compatible storage. Returns S3 key or None if skipped."""
    client = _get_s3_client()
    if not client:
        logger.warning("S3 credentials not configured — skipping upload for %s", db_name)
        return None

    bucket = os.getenv("BACKUP_S3_BUCKET", "backups")
    prefix = os.getenv("BACKUP_S3_PREFIX", "postgres/").rstrip("/")
    filename = os.path.basename(local_path)
    s3_key = f"{prefix}/{db_name}/{filename}"

    logger.info("Uploading %s → s3://%s/%s", filename, bucket, s3_key)
    client.upload_file(local_path, bucket, s3_key)
    logger.info("Upload complete: %s", s3_key)
    return s3_key


@task(name="prune-old-backups")
def prune_old_backups(db_name: str, retention_days: int) -> int:
    """Delete backups older than retention_days. Returns count of deleted objects."""
    client = _get_s3_client()
    if not client:
        return 0

    bucket = os.getenv("BACKUP_S3_BUCKET", "backups")
    prefix = os.getenv("BACKUP_S3_PREFIX", "postgres/").rstrip("/")
    db_prefix = f"{prefix}/{db_name}/"

    now = datetime.now(timezone.utc)
    deleted = 0

    try:
        paginator = client.get_paginator("list_objects_v2")
        for page in paginator.paginate(Bucket=bucket, Prefix=db_prefix):
            for obj in page.get("Contents", []):
                age_days = (now - obj["LastModified"]).days
                if age_days > retention_days:
                    client.delete_object(Bucket=bucket, Key=obj["Key"])
                    logger.info("Pruned old backup: %s (%d days old)", obj["Key"], age_days)
                    deleted += 1
    except Exception as exc:
        logger.warning("Prune failed (non-critical): %s", exc)

    return deleted


@task(name="send-backup-notification")
def send_backup_notification(results: list[dict[str, Any]], success: bool) -> None:
    """Send Telegram notification with backup summary."""
    from integrations.telegram import notify_telegram

    if success:
        lines = ["✅ *Postgres backup complete*\n"]
        for r in results:
            size_str = f"{r['size_mb']:.1f}MB" if r.get("size_mb") else "?"
            uploaded = "☁️ uploaded" if r.get("s3_key") else "⚠️ local only"
            lines.append(f"• `{r['db_name']}` — {size_str} — {uploaded}")
        next_run = "Tomorrow 03:00 UTC"
        lines.append(f"\nNext backup: {next_run}")
        message = "\n".join(lines)
    else:
        message = "❌ *Postgres backup FAILED* — check Prefect logs"

    notify_telegram(message)


# ---------------------------------------------------------------------------
# Main backup flow
# ---------------------------------------------------------------------------

@flow(name="backup-postgres", log_prints=True)
def backup_postgres(
    databases: dict[str, str] | None = None,
    retention_days: int | None = None,
    notify: bool = True,
) -> list[dict[str, Any]]:
    """Back up Postgres databases to S3-compatible storage.

    Args:
        databases: Mapping of {db_name: s3_path_suffix}. Default: Prefect Variable.
        retention_days: Days to keep backups. Default: Prefect Variable or 30.
        notify: Send Telegram notification on completion.
    """
    # Resolve config — parameters override Prefect Variables
    if databases is None:
        try:
            databases = Variable.get("backup_databases", default={"second_brain": "second_brain", "prefect": "prefect"})
        except Exception:
            databases = {"second_brain": "second_brain", "prefect": "prefect"}

    if retention_days is None:
        try:
            retention_days = int(Variable.get("backup_retention_days", default=30))
        except Exception:
            retention_days = int(os.getenv("BACKUP_RETENTION_DAYS", "30"))

    results = []
    success = True

    with tempfile.TemporaryDirectory() as tmpdir:
        for db_name in databases:
            try:
                dump_path = dump_database(db_name, tmpdir)
                size_mb = os.path.getsize(dump_path) / 1024 / 1024
                s3_key = upload_to_s3(dump_path, db_name)
                pruned = prune_old_backups(db_name, retention_days)

                results.append({
                    "db_name": db_name,
                    "dump_path": dump_path,
                    "size_mb": size_mb,
                    "s3_key": s3_key,
                    "pruned": pruned,
                })
                logger.info("Backup complete for %s (%.1f MB, pruned %d)", db_name, size_mb, pruned)

            except Exception as exc:
                logger.error("Backup failed for %s: %s", db_name, exc)
                success = False
                results.append({"db_name": db_name, "error": str(exc)})

    if notify:
        send_backup_notification(results, success)

    if not success:
        raise RuntimeError("One or more backups failed — see logs")

    return results


# ---------------------------------------------------------------------------
# Restore flow
# ---------------------------------------------------------------------------

@flow(name="restore-postgres-from-backup", log_prints=True)
def restore_postgres_from_backup(
    database: str,
    backup_key: str = "latest",
    dry_run: bool = True,
) -> dict[str, Any]:
    """Restore a Postgres database from an S3 backup.

    Args:
        database: Database name to restore (e.g. "second_brain").
        backup_key: S3 key of the backup, or "latest" to auto-select most recent.
        dry_run: If True (default), only show what would be restored — does NOT restore.
                 Must explicitly pass dry_run=False to actually restore.
    """
    from shared.config import load_settings
    from integrations.telegram import send_telegram_message

    client = _get_s3_client()
    if not client:
        raise RuntimeError("S3 credentials not configured — cannot restore")

    bucket = os.getenv("BACKUP_S3_BUCKET", "backups")
    prefix = os.getenv("BACKUP_S3_PREFIX", "postgres/").rstrip("/")
    db_prefix = f"{prefix}/{database}/"

    # Resolve "latest"
    if backup_key == "latest":
        paginator = client.get_paginator("list_objects_v2")
        objects = []
        for page in paginator.paginate(Bucket=bucket, Prefix=db_prefix):
            objects.extend(page.get("Contents", []))
        if not objects:
            raise RuntimeError(f"No backups found for {database} at s3://{bucket}/{db_prefix}")
        objects.sort(key=lambda o: o["LastModified"], reverse=True)
        backup_key = objects[0]["Key"]
        logger.info("Latest backup: %s (modified %s)", backup_key, objects[0]["LastModified"])

    if dry_run:
        logger.info("[DRY RUN] Would restore %s from s3://%s/%s", database, bucket, backup_key)
        return {"dry_run": True, "database": database, "backup_key": backup_key}

    # Download and restore
    with tempfile.TemporaryDirectory() as tmpdir:
        local_path = f"{tmpdir}/{os.path.basename(backup_key)}"
        logger.info("Downloading %s...", backup_key)
        client.download_file(bucket, backup_key, local_path)

        dsn = _get_dsn(database)
        parts = _parse_dsn(dsn)
        env = os.environ.copy()
        env["PGPASSWORD"] = parts["password"]

        # Decompress
        sql_path = local_path.replace(".gz", "")
        with gzip.open(local_path, "rb") as f_in:
            with open(sql_path, "wb") as f_out:
                shutil.copyfileobj(f_in, f_out)

        cmd = [
            "psql",
            "-h", parts["host"],
            "-p", parts["port"],
            "-U", parts["user"],
            "-d", parts["dbname"],
            "--no-password",
            "-f", sql_path,
        ]

        logger.info("Restoring %s from %s...", database, backup_key)
        result = subprocess.run(cmd, capture_output=True, env=env)

        if result.returncode != 0:
            raise RuntimeError(f"psql restore failed: {result.stderr.decode()[:500]}")

        logger.info("Restore complete for %s", database)

        from integrations.telegram import notify_telegram
        notify_telegram(f"✅ *Postgres restore complete*\n• `{database}` restored from `{os.path.basename(backup_key)}`")

    return {"dry_run": False, "database": database, "backup_key": backup_key}


if __name__ == "__main__":
    backup_postgres()
