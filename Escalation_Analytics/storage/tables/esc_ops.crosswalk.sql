--DROP TABLE esc_ops.crosswalk;
CREATE TABLE IF NOT EXISTS esc_ops.crosswalk
(
	cc_case_id VARCHAR(25)   ENCODE RAW
	,ticket_guid VARCHAR(50)   ENCODE RAW
	,ticket_id VARCHAR(50)   ENCODE RAW
)
DISTSTYLE AUTO
 DISTKEY (ticket_id)
 SORTKEY (
	cc_case_id
	, ticket_guid
	, ticket_id
	)
;
ALTER TABLE esc_ops.crosswalk owner to admin;


