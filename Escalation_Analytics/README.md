# Escalation Analytics Pipeline

Event-driven pipeline that unloads Remedy escalation data to S3, processes it via catalog refresh, refreshes Redshift marts, and serves dashboards in Amazon QuickSight for EscalationTeams and leadership.

## Flow
1) **Unload**: EventBridge schedule (01:00) triggers Lambda dwunloader.py to unload source data to S3 and drop a readykey marker.
2) **Trigger**: EventBridge rule listens for readykey and starts a Step Functions state machine.
3) **Process**: State machine runs Glue crawler DWCrawler to catalog staged unload outputs.
4) **Refresh**: State machine calls Redshift CALL public.sp_datarefresh() to load esc_ops tables and public views.
5) **Consume**: QuickSight dashboards read from public views; key metrics include bounce counts, team ownership timeframes, and agent aggregates.

## Repository map
- data_sources.md ? source systems and unload definitions.
- ingestion/ ? S3 unload SQL and external table DDL; Lambda unloader.
- storage/ ? core Redshift tables (esc_ops).
- 	ransformations/ ? stored procedures (refresh logic) and consumer-facing views.
- orchestration/ ? EventBridge, Step Functions, Glue crawler, Redshift Data API assets.
- examples/ ? lineage diagrams
- docs/ ? dataflow diagram.


Identifiers use anonymized example values; Account ID and Redshift CLUSTER_ID are intentionally excluded for security of owning systems.
