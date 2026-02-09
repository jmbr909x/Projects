CREATE EXTERNAL TABLE ext_src.remedy_esc_audittrail(assigned_to string
, audit_eid string
, case_id string
, created_date string
, created_by string
, description string
, from_string string
, module string
, to_string string
, "type" string
, esc_issue_guid string
, initial_case_id string
, parent_case_id string
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat' OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
WITH SERDEPROPERTIES ( 'serialization.format'='1')
LOCATION 's3://example-escalation-data/remedy_esc_audittrail/'
;

