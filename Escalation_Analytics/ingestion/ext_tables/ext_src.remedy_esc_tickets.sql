CREATE EXTERNAL TABLE ext_src.remedy_esc_tickets(create_date string
, create_day string
, case_id string
, arrived_date string
, asin_list string
, assigned_date string
, assigned_to_group string
, assigned_to_individual string
, assignee_manager_login string
, case_type string
, category string
, department string
, details string
, impact string
, impact_label string
, item string
, last_modified_by string
, modified_date string
, pending_reason string
, queue_number string
, release_version string
, requester_login string
, requester_login_name string
, requester_name string
, resolved_by string
, resolved_date string
, root_cause string 
, root_cause_details string 
, short_description string
, status string
, submitted_by string
, "type" string
, resolver_group_mgr_name string
, assigned_dept_name string
, esc_issue_alias string
, initial_case_id string
, esc_issue_guid string
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat' OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
WITH SERDEPROPERTIES ( 'serialization.format'='1')
LOCATION 's3://example-escalation-data/remedy_esc_tickets/'
;

