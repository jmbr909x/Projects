--DROP TABLE esc_ops.case_annotation;
CREATE TABLE IF NOT EXISTS esc_ops.case_annotation
(
	case_id VARCHAR(25)   ENCODE RAW
	,created_by VARCHAR(50)   ENCODE RAW
	,created_time TIMESTAMP WITHOUT TIME ZONE   ENCODE RAW
)
DISTSTYLE AUTO
 SORTKEY (
	case_id
	, created_by
	, created_time
	)
;
ALTER TABLE esc_ops.case_annotation owner to admin;

