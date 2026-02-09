CREATE EXTERNAL TABLE ext_src.case_remedy_esc_tickets(case_id string
, related_item_type string
, ticket_id string
, region_id string
, associating_agent_login_id string
, is_active string
, ticket_add_date string
, created_by string
, creation_date string
, last_updated string
, last_updated_by string
, last_updated_date string
, ticket_id_original string
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat' OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
WITH SERDEPROPERTIES ( 'serialization.format'='1')
LOCATION 's3://example-escalation-data/case_remedy_esc_tickets/'
;
