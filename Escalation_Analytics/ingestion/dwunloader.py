"""
Environment variables (override defaults in code as needed):
  BUCKET      : S3 bucket containing SQL files and unload targets.
  SQL_PREFIX  : Prefix for .sql files (default: 'unload-sql/').
  READY_KEY   : Marker key to write on success (default: 'readykey').
  CLUSTER_ID  : Redshift provisioned cluster identifier (required).
  DATABASE    : Target database name.
  SECRET_ARN  : Secrets Manager ARN for credentials
Execution derived Variables
  {START_DATE} -> previous day 00:00:00 in TZ
  {END_DATE}   -> previous day 23:59:59 in TZ
  {RUN_TS}     -> execution timestamp YYYYMMDD_HHMMSS in TZ
"""

import boto3
import os
from datetime import datetime, timedelta, timezone

try:  # Python 3.9+ in Lambda
    from zoneinfo import ZoneInfo
except ImportError:  # pragma: no cover - fallback not expected on Lambda
    from backports.zoneinfo import ZoneInfo  # type: ignore

s3 = boto3.client("s3")
rsd = boto3.client("redshift-data")

BUCKET = os.environ.get("BUCKET", "example-escalation-data")
SQL_PREFIX = os.environ.get("SQL_PREFIX", "unload-sql/")
READY_KEY = os.environ.get("READY_KEY", "readykey")
CLUSTER_ID = os.environ.get("CLUSTER_ID", "")
DATABASE = os.environ.get("DATABASE", "analyticsdb")
SECRET_ARN = os.environ.get("SECRET_ARN", "arn:aws:secretsmanager:us-east-1:ACCOUNT_ID:secret:redshift/esc")
TZ = os.environ.get("TZ", "UTC")


# Get the current time in the configured timezone.
def _tz_now():
    tz = timezone.utc if TZ.upper() == "UTC" else ZoneInfo(TZ)
    return datetime.now(tz)


# Compute yesterday's start/end window and the run timestamp string.
def _day_window(dt):
    yday = (dt - timedelta(days=1)).date()
    start = f"{yday} 00:00:00"
    end = f"{yday} 23:59:59"
    run_ts = dt.strftime("%Y%m%d_%H%M%S")
    return start, end, run_ts


# List all .sql object keys under the configured prefix.
def _list_sql_keys():
    keys = []
    token = None
    while True:
        if token:
            resp = s3.list_objects_v2(Bucket=BUCKET, Prefix=SQL_PREFIX, ContinuationToken=token)
        else:
            resp = s3.list_objects_v2(Bucket=BUCKET, Prefix=SQL_PREFIX)
        for obj in resp.get("Contents", []):
            if obj["Key"].lower().endswith(".sql"):
                keys.append(obj["Key"])
        if resp.get("IsTruncated"):
            token = resp.get("NextContinuationToken")
        else:
            break
    return keys


# Read a SQL file from S3 and substitute runtime parameters.
def _render_sql(key, start_ts, end_ts, run_ts):
    body = s3.get_object(Bucket=BUCKET, Key=key)["Body"].read().decode("utf-8")
    return (
        body
        .replace("{START_DATE}", start_ts)
        .replace("{END_DATE}", end_ts)
        .replace("{RUN_TS}", run_ts)
    )


# Execute a SQL string via Redshift Data API against a provisioned cluster.
def _exec_sql(sql):
    params = {
        "Database": DATABASE,
        "Sql": sql,
    }
    if SECRET_ARN:
        params["SecretArn"] = SECRET_ARN
    if CLUSTER_ID:
        params["ClusterIdentifier"] = CLUSTER_ID
    else:
        raise RuntimeError("Provide CLUSTER_ID for provisioned Redshift")
    resp = rsd.execute_statement(**params)
    return resp["Id"]


# Poll a statement until it finishes or fails.
def _wait(stmt_id):
    while True:
        st = rsd.describe_statement(Id=stmt_id)
        status = st["Status"]
        if status in ("FAILED", "ABORTED"):
            raise RuntimeError(f"Statement {stmt_id} {status}: {st}")
        if status == "FINISHED":
            return


# Lambda entrypoint: run all unloads, wait, then write ready marker.
def handler(event, context):
    now = _tz_now()
    start_ts, end_ts, run_ts = _day_window(now)

    statement_ids = []
    for key in _list_sql_keys():
        sql = _render_sql(key, start_ts, end_ts, run_ts)
        statement_ids.append(_exec_sql(sql))

    for sid in statement_ids:
        _wait(sid)

    # All statements finished successfully; write marker
    s3.put_object(Bucket=BUCKET, Key=READY_KEY, Body=b"")

    return {
        "run_ts": run_ts,
        "start_date": start_ts,
        "end_date": end_ts,
        "files": len(statement_ids),
        "readykey": READY_KEY,
    }


if __name__ == "__main__":
    print(handler({}, None))
