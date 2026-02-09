--DROP TABLE esc_ops.remedy_esc_tickets;
CREATE TABLE IF NOT EXISTS esc_ops.remedy_esc_tickets
(
	rn BIGINT   ENCODE az64
	,create_date TIMESTAMP WITHOUT TIME ZONE   ENCODE RAW
	,case_id VARCHAR(50)   ENCODE RAW
	,assigned_to_group VARCHAR(50)   ENCODE RAW
	,assigned_to_individual VARCHAR(50)   ENCODE lzo
	,assignee_manager_login VARCHAR(25)   ENCODE lzo
	,department VARCHAR(50)   ENCODE lzo
	,impact_label VARCHAR(50)   ENCODE lzo
	,item VARCHAR(100)   ENCODE lzo
	,modified_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,requester_login VARCHAR(150)   ENCODE lzo
	,resolved_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,resolved_by VARCHAR(150)   ENCODE lzo
	,status VARCHAR(25)   ENCODE lzo
	,root_cause VARCHAR(100)   ENCODE lzo
	,root_cause_details VARCHAR(500)   ENCODE lzo
	,submitted_by VARCHAR(50)   ENCODE lzo
	,"type" VARCHAR(50)   ENCODE lzo
	,esc_issue_alias VARCHAR(50)   ENCODE lzo
	,short_description VARCHAR(500)   ENCODE lzo
)
DISTSTYLE AUTO
 DISTKEY (esc_issue_alias)
 SORTKEY (
	case_id
	, create_date
	, assigned_to_group
	)
;
ALTER TABLE esc_ops.remedy_esc_tickets owner to admin;



