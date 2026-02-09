# Medallion Structure

This repository follows a three-layer medallion model mapped to existing assets:

- **Bronze (Landing/Staging)**
  - S3 unload outputs written daily to `s3://example-escalation-data/<dataset>/{RUN_TS}/` by `ingestion/unload_sql/*.sql` via `dwunloader.py`.
  - External tables under `ingestion/ext_tables/` (schema `ext_src`) read the raw Parquet files.

- **Silver (Cleansed/Core)**
  - Redshift core tables in schema `esc_ops` defined in `storage/tables/*.sql`.
  - Populated by stored procedures in `transformations/stored_procs/*.sql` (`sp_*`) that pull from `ext_src`/staging inputs.

- **Gold (Serving/Analytics)**
  - Consumer-facing views in schema `public` under `transformations/views/*.sql` (e.g., `vw_highbounce`, `vw_ticket_group_assignment_breakout_linked`, `vw_combined_wl_e2e`).
  - Lineage diagrams for each view live in `examples/lineage/*.mmd`.

## Flow alignment
1. Bronze: S3 unloads + external tables (EventBridge @01:00, Lambda `dwunloader.py`).
2. Silver: Stored procedures refresh `esc_ops` tables after Glue crawler catalogs the data.
3. Gold: Views expose analytics for QuickSight dashboards.

## Operational notes
- Single bucket: `s3://example-escalation-data/` for all landing paths.
- Daily cadence with next-day freshness; catalog refresh via crawler `DWCrawler`; Redshift refresh via `public.sp_datarefresh()`.
- Account ID and cluster ID are intentionally excluded to preserve security of owning environment;
