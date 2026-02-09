# Architecture & Dataflow

See `Dataflow.mmd` for the end-to-end path: EventBridge (schedule) → Lambda unload to S3 → readykey → Step Functions → Glue crawler → Redshift `sp_datarefresh` → public views → QuickSight.

Medallion breakdown is detailed in `medallion.md`.
