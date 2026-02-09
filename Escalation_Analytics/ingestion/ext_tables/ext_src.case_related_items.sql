CREATE EXTERNAL TABLE ext_src.case_related_items(case_id string
, related_item_id string
, related_item_type string
, associating_agent_login_id string
, is_active string
, partition_key string
, related_item_add_date string
, created_by string
, creation_date string
, last_updated string
, last_updated_by string
, last_updated_date string
, source string
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat' OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
WITH SERDEPROPERTIES ( 'serialization.format'='1')
LOCATION 's3://example-escalation-data/case_related_items/'
;
