# Escalation Analytics Pipeline

## Problem Context
Decision‑making for escalations is distorted because all case‑to‑ticket resolution latency is currently attributed to the final EscOps ticket owner, masking delays introduced by Dev_teams routing, reassignment, and inactivity. We need an ESC data warehouse (Bronze/Silver/Gold) that reconstructs ticket ownership timelines from audit logs, decomposes latency by Dev_teams, quantifies routing “bounce,” and exposes real‑time views of active escalations so accountability and process improvements target the true sources of delay.

## Architectural Overview
- Event-driven daily pipeline (01:00) — EventBridge schedule triggers a Lambda to run unload SQL, write datasets to S3 with a ready-key marker.
- Ready-key event starts a Step Functions state machine; it runs a Glue crawler to refresh the catalog, then calls Redshift public.sp_datarefresh() via Redshift Data API; IAM policies cover Step Functions and Redshift calls.
- Medallion model: Bronze = external tables over S3 unloads; Silver = core Redshift tables in esc_ops schema loaded by stored procedures; Gold = consumer-facing views in public schema for analytics.
- Analytics: QuickSight dashboards read the Gold views for ownership timelines, bounce counts, workload, and agent aggregates.
- Diagrams: Mermaid dataflow and per-view lineage diagrams illustrate the end-to-end flow.
- Operations: Daily cadence with next-day freshness; recommended alerting via CloudWatch/SNS for Step Functions failures or missed runs.

## My role
This entire process and components were developed solely by me based on personally derived goals and identified gaps in metrics calculations for determination of latency contributors.

## Flow
1) **Unload**: EventBridge schedule (01:00) triggers Lambda dwunloader.py to unload source data to S3 and drop a readykey marker.
2) **Trigger**: EventBridge rule listens for readykey and starts a Step Functions state machine.
3) **Process**: State machine runs Glue crawler DWCrawler to catalog staged unload outputs.
4) **Refresh**: State machine calls Redshift CALL public.sp_datarefresh() to load esc_ops tables and public views.
5) **Consume**: QuickSight dashboards read from public views; key metrics include bounce counts, team ownership timeframes, and agent aggregates.

## Technology Stack
EventBridge, Lambda, S3, Glue, Redshift, Step Functions, QuickSight

## Repository map
- data_sources.md  source systems and unload definitions.
- ingestion/  S3 unload SQL and external table DDL; Lambda unloader.
- storage/  core Redshift tables (esc_ops).
- Transformations/  stored procedures (refresh logic) and consumer-facing views.
- orchestration/  EventBridge, Step Functions, Glue crawler, Redshift Data API assets.
- lineage diagrams/  illustrated dataflow lineage
- docs/  dataflow diagram and medallion breakout


Identifiers use anonymized example values; Account ID and Redshift CLUSTER_ID are intentionally excluded for security of owning systems.
