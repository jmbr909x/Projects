--DROP TABLE esc_ops.remedy_esc_audittrail;
CREATE TABLE IF NOT EXISTS esc_ops.remedy_esc_audittrail
(
	assigned_to VARCHAR(50)   ENCODE lzo
	,audit_eid VARCHAR(100)   ENCODE lzo
	,case_id VARCHAR(50)   ENCODE RAW
	,created_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,created_by VARCHAR(150)   ENCODE lzo
	,description VARCHAR(25)   ENCODE bytedict
	,from_string VARCHAR(100)   ENCODE RAW
	,to_string VARCHAR(100)   ENCODE RAW
	,"type" VARCHAR(10)   ENCODE bytedict
	,esc_issue_guid VARCHAR(50)   ENCODE lzo
	,initial_case_id VARCHAR(50)   ENCODE lzo
	,parent_case_id VARCHAR(50)   ENCODE lzo
)
DISTSTYLE AUTO
 SORTKEY (
	case_id
	, to_string
	, from_string
	)
;
ALTER TABLE esc_ops.remedy_esc_audittrail owner to admin;



