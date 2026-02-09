# data_sources.md

## Source systems
- Remedy escalation/ticketing datasets unloaded via SQL (see `ingestion/unload_sql/*.sql`).

## Unload definitions
- `annotation_data_unload.sql`
- `case_data_unload.sql`
- `case_related_items_unload.sql`
- `case_remedy_esc_tickets_unload.sql`
- `remedy_esc_audittrail_unload.sql`
- `remedy_esc_tickets_unload.sql`

## External landing schema
- External tables under `ingestion/ext_tables/` define staged Parquet/CSV structures mapped to S3 unload outputs.

## Freshness
- Daily unload scheduled at 01:00 via EventBridge; data expected to be available same day after Glue + `sp_datarefresh`.
