# Orchestration (Step Functions)

Assets:
- `state_machine.asl.json` — Amazon States Language definition to run Glue crawler `DWCrawler` then `CALL public.sp_datarefresh()`, with wait/poll and marker cleanup.
- `IAM_policy_step_functions.json` — Inline policy for the Step Functions execution role.

Console/CLI:
- Create role (trust = `states.amazonaws.com`), attach `IAM_policy_step_functions.json`.
- Create Standard state machine with definition from `state_machine.asl.json`; enable CloudWatch Logs.
- Create/choose SNS topic for alerts and set the topic ARN in `state_machine.asl.json`; ensure subscribers (email/SMS/ops queue) are confirmed.
- Target: provisioned Redshift cluster (`ClusterIdentifier` is set); serverless workgroup is not used.

## Schedule & SLA (reference)
- Upstream EventBridge schedule triggers the unload/Lambda at 01:00 daily; this state machine is expected to run once per day off the ready-key event.
- Freshness: Same-day completion after the 01:00 trigger (next-day data available to consumers).
- Alerting: CloudWatch alarms on `ExecutionsFailed`, on “no execution in 24h,” and on Redshift Data API errors for the provisioned cluster; route to the SNS topic configured in the ASL definition.
