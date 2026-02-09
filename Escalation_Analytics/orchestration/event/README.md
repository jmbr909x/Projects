# Event (Ready Key Trigger)

Purpose: wire the S3 `readykey` marker (written by `dwunloader.py`) to Step Functions via EventBridge.

Files:
- `eventbridge_rule.json`  Event pattern for S3 Object Created where key suffix = `readykey`.
- `put_targets.json`  Target definition to start the Step Functions state machine.

## Schedule & SLA
- Expected cadence: EventBridge schedule runs the unload/Lambda at 01:00 daily; this ready-key path should fire once per day.
- Freshness target: Next-day availability; Glue + `sp_datarefresh` should complete the same day as the 01:00 trigger.
- Alerting (recommended): CloudWatch alarms on `ExecutionsFailed` and on ?no execution in 24h?, routed to the SNS topic configured in the state machine.
