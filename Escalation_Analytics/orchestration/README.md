# orchestration

- event/ EventBridge rule and target for readykey.
- state_machine/ Step Functions ASL (crawler DWCrawler -> Redshift refresh) and IAM policy.
- 
Refresh/ Redshift Data API call SQL and IAM policy.

Cadence: daily at 01:00 (schedule triggers unload; readykey triggers state machine); freshness expected same day.

Account ID is intentionally omitted in ARNs for security; set it at deployment.
