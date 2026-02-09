UNLOAD (
    $$
    Select *
    from dw.remedy_sim_audittrail
    where cast(created_date AS timestamp) BETWEEN cast('{START_DATE}' AS timestamp)
        AND cast('{END_DATE}' AS timestamp)
        and description IN (
            'Status',
            'Correspondence',
            'Category',
            'Type',
            'Item',
            'Assigned Ind.',
            'Assigned Group',
            'Tag'
        )
        and case_id in (
            select case_id
            from dw.o_remedy_sim_audittrail
            where description = 'Assigned Group'
                and (
                    to_string IN (
                        'EscOps'
                    )
                    or from_string IN (
                        'EscOps'
                    )
                )
                and cast(created_date AS timestamp) BETWEEN cast('{START_DATE}' AS timestamp)
                AND cast('{END_DATE}' AS timestamp)
        ) $$
) TO 's3://example-escalation-data/remedy_esc_audittrail/{RUN_TS}/' 
IAM_ROLE default 
FORMAT AS PARQUET 
MAXFILESIZE 1000 MB 
ALLOWOVERWRITE PARALLEL ON;
