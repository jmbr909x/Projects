UNLOAD (
    $$
    Select *
    from cds.annotation_data
    where (
            content like '%### EscOps Action ###%'
        )
        and cast(created_time AS timestamp) BETWEEN cast('{START_DATE}' AS timestamp)
        AND cast('{END_DATE}' AS timestamp) $$
) TO 's3://example-escalation-data/annotation_data/{RUN_TS}/' 
IAM_ROLE 'arn:aws:iam::ACCOUNT_ID:role/RedshiftUnloadRole' 
FORMAT AS PARQUET 
MAXFILESIZE 1000 MB 
ALLOWOVERWRITE PARALLEL ON;
