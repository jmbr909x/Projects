UNLOAD (
    $$
    select *
    from dw.case_related_items
    WHERE cast(last_updated AS timestamp) BETWEEN cast('{START_DATE}' AS timestamp)
        AND cast('{END_DATE}' AS timestamp) $$
) TO 's3://example-escalation-data/case_related_items/{RUN_TS}/' 
IAM_ROLE default
FORMAT AS PARQUET 
MAXFILESIZE 1000 MB 
ALLOWOVERWRITE PARALLEL ON;
