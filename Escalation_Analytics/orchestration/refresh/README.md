# Refresh (Redshift Data API)

Files:
- `execute_sp_datarefresh.sql` — statement executed by Step Functions via Redshift Data API.
- `IAM_policy_redshift_data.json` — permissions for whichever role calls Redshift (Step Functions execution role or Lambda).

Ensure the secret ARN in the policy matches the credential used to execute `public.sp_datarefresh`.
