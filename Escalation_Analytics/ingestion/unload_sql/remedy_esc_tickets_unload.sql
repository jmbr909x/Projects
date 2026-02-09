-- Unload generated from remedy_esc_tickets
-- Set run_ts (e.g., 20260208_153000) before executing
UNLOAD (
    $$
    Select *
    from aws_kumo.o_remedy_sim_tickets
    WHERE cast(modified_date AS timestamp) BETWEEN cast('{START_DATE}' AS timestamp)
        AND cast('{END_DATE}' AS timestamp)
        and case_id in (
            select case_id
            from aws_kumo.o_remedy_sim_audittrail
            where description = 'Assigned Group'
                and (
                    to_string IN (
                        'Support Operations Redshift',
                        'AWS Support Operations RS',
                        'CookieMonster_ControlPlane',
                        'CookieMonster_DataPlane',
                        'CookieMonster_Dory',
                        'CookieMonster_Tidal',
                        'AWS Redshift DBE Escalation',
                        'AWS CookieMonster DataPlane',
                        'AWS CookieMonster ControlPlane'
                    )
                    or from_string IN (
                        'Support Operations Redshift',
                        'AWS Support Operations RS',
                        'CookieMonster_ControlPlane',
                        'CookieMonster_DataPlane',
                        'CookieMonster_Dory',
                        'CookieMonster_Tidal',
                        'AWS Redshift DBE Escalation',
                        'AWS CookieMonster DataPlane',
                        'AWS CookieMonster ControlPlane'
                    )
                )
                and cast(created_date AS timestamp) BETWEEN cast('{START_DATE}' AS timestamp)
                AND cast('{END_DATE}' AS timestamp)
        ) $$
) TO 's3://example-escalation-data/remedy_esc_tickets/{RUN_TS}/' IAM_ROLE 'arn:aws:iam::ACCOUNT_ID:role/RedshiftUnloadRole' FORMAT AS PARQUET MAXFILESIZE 1000 MB ALLOWOVERWRITE PARALLEL ON;
