--DROP TABLE esc_ops.annotation_data;
CREATE TABLE IF NOT EXISTS esc_ops.annotation_data
(
	case_id VARCHAR(50)   ENCODE RAW
	,communication_type VARCHAR(50)   ENCODE lzo
	,content VARCHAR(16383)   ENCODE lzo
	,created_by VARCHAR(50)   ENCODE RAW
	,created_time TIMESTAMP WITHOUT TIME ZONE   ENCODE RAW
	,content_ext VARCHAR(16383)   ENCODE lzo
	,tenant_id VARCHAR(50)   ENCODE lzo
)
DISTSTYLE AUTO
 SORTKEY (
	case_id
	, created_by
	, created_time
	)
;
ALTER TABLE esc_ops.annotation_data owner to admin;

