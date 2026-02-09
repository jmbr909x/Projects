--DROP TABLE esc_ops.service_sme;
CREATE TABLE IF NOT EXISTS esc_ops.service_sme
(
	eng_alias VARCHAR(25)   ENCODE RAW
	, site VARCHAR(10)   ENCODE RAW
	, manager VARCHAR(25)   ENCODE lzo
)
DISTSTYLE AUTO
 SORTKEY (
	eng_alias
	, site
	)
;
ALTER TABLE esc_ops.service_sme owner to admin;


