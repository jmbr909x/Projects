CREATE EXTERNAL TABLE ext_src.case_data(case_id string
, status string
, assigned_agent_login string
, subject_ext string
, initial_queue string
, queue string
, creation_date string
, first_outbound_date string
, last_inbound_date string
, last_outbound_date string
, last_updated_date string
, next_response_expiration_date string
, response_sla_start_date string
, response_sla_minutes string
, sla_expiration_date string
, start_date string
, status_sla_expiration_date string
, operation string
, message_timestamp string
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat' OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
WITH SERDEPROPERTIES ( 'serialization.format'='1')
LOCATION 's3://example-escalation-data/case_data/'
;
