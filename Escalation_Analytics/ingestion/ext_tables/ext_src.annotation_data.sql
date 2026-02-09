CREATE EXTERNAL TABLE ext_src.annotation_data(case_id string
, communication_id string
, communication_type string
, content string
, created_by string
, created_time string
, edited_by string
, edited_time string
, force_write string
, internal string
, outbound string
, phone_number string
, rating string
, time_to_call string
, version_number string
, operation string
, event_id string
, namespace string
, message_timestamp string
, session_source string
, phone_country_code string
, linked_communication_id string
, phone_extension string
, data_processed_region string
, data_processed_time string
, content_ext string
, tenant_id string
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat' OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
WITH SERDEPROPERTIES ( 'serialization.format'='1')
LOCATION 's3://example-escalation-data/annotation_data/'
;
