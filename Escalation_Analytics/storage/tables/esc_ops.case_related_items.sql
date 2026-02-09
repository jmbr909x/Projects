--DROP TABLE esc_ops.case_related_items;
CREATE TABLE IF NOT EXISTS esc_ops.case_related_items
(
	case_id VARCHAR(25)   ENCODE RAW
	,related_item_id VARCHAR(50)   ENCODE RAW
	,related_item_type VARCHAR(25)   ENCODE lzo
	,associating_agent_login_id VARCHAR(50)   ENCODE lzo
	,creation_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,initial_queue VARCHAR(100)   ENCODE lzo
	,queue VARCHAR(100)   ENCODE lzo
)
DISTSTYLE AUTO
 DISTKEY (case_id)
 SORTKEY (
	case_id
	, related_item_id
	)
;
ALTER TABLE esc_ops.case_related_items owner to admin;


