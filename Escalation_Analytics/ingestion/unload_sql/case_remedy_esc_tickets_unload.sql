UNLOAD ($$
WITH soesc AS (
    select case_id
    from dw.case_data
    where cast(last_updated_date AS timestamp) BETWEEN cast('{START_DATE}' AS timestamp)
        AND cast('{END_DATE}' AS timestamp)
        and (
            initial_queue in (
                    'support-tier1',
                    'support-tier2',
                    'support-tier3'
                )
            and operation = 'AddCaseAgent'
        )
        or (
            queue in (
                    'support-tier1',
                    'support-tier2',
                    'support-tier3'
                )
        )
),
id_join AS (
        SELECT case_id,
            sim_issue_alias
        FROM dw.o_remedy_sim_tickets
        WHERE cast(modified_date AS timestamp) BETWEEN cast('{START_DATE}' AS timestamp)
            AND cast('{END_DATE}' AS timestamp)
            and case_id IN (
                select case_id
                from dw.o_remedy_sim_audittrail
                where (
                        to_string IN (
                            'Escops'
                        )
                        or from_string IN (
                            'Escops'
                        )
                    )
                    and cast(created_date AS timestamp) BETWEEN cast('{START_DATE}' AS timestamp)
                    AND cast('{END_DATE}' AS timestamp)
                    and "description" in (
                        'Status',
                        'Correspondence',
                        'Assigned Group',
                        'Category',
                        'Type',
                        'Item',
                        'Assigned Ind.'
                    )
            )
    ),
    esc_case as (
        select cc.case_id as cc_case_id,
            cc.ticket_id as tt_guid,
            id.sim_issue_alias as tt_id
        from dw.case_remedy_sim_tickets cc
            Right join id_join id on id.case_id = cc.ticket_id
    ),
    soesc2 as (
        select case_id
        from dw.case_data
        where cast(last_updated_date AS timestamp) BETWEEN cast('{START_DATE}' AS timestamp)
            AND cast('{END_DATE}' AS timestamp)
            and case_id in (
                select cc_case_id
                from esc_case
            )
            and (
                initial_queue not in (
                    'support-tier1',
                    'support-tier2',
                    'support-tier3'
                )
                and operation = 'AddCaseAgent'
            )
            and (
                queue not in (
                    'support-tier1',
                    'support-tier2',
                    'support-tier3'
                )
            )
    ),
    case_check as (
        select case_id as cc_case_id
        from soesc
        union all
        select case_id as cc_case_id
        from soesc2
    )
Select *
from dw.case_remedy_sim_tickets
WHERE case_id in (
        select cc_case_id
        from case_check
    )
    and cast(last_updated AS timestamp) BETWEEN cast('{START_DATE}' AS timestamp)
    AND cast('{END_DATE}' AS timestamp)
$$)
TO 's3://example-escalation-data/case_remedy_esc_tickets/{RUN_TS}/'
IAM_ROLE default
FORMAT AS PARQUET
MAXFILESIZE 1000 MB
ALLOWOVERWRITE
PARALLEL ON;
