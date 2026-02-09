# ingestion

- `unload_sql/` queries to extract Remedy escalation data to S3.
- `ext_tables/` external table DDL pointing to unloaded files.
- `dwunloader.py` Lambda that runs the unloads and writes a `readykey` marker.

Trigger: EventBridge schedule at 01:00 daily.

Note: The Redshift `CLUSTER_ID` environment variable is intentionally not embedded in code or docs for security/privacy; set it at deployment time.
