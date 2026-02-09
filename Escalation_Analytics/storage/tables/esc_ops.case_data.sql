--DROP TABLE esc_ops.case_data;
CREATE TABLE IF NOT EXISTS esc_ops.case_data
(
	case_id VARCHAR(25)   ENCODE RAW
	,status VARCHAR(25)   ENCODE bytedict
	,assigned_agent_login VARCHAR(25)   ENCODE RAW
	,initial_queue VARCHAR(100)   ENCODE bytedict
	,queue VARCHAR(100)   ENCODE bytedict
	,creation_date TIMESTAMP WITHOUT TIME ZONE   ENCODE RAW
	,first_outbound_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,last_inbound_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,last_outbound_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,last_updated_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,next_response_expiration_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,response_sla_start_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,response_sla_minutes INTEGER   ENCODE az64
	,sla_expiration_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,start_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,status_sla_expiration_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,operation VARCHAR(25)   ENCODE bytedict
	,message_timestamp TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
)
DISTSTYLE AUTO
 DISTKEY (case_id)
 SORTKEY (
	case_id
	, assigned_agent_login
	, creation_date
	)
;
ALTER TABLE esc_ops.case_data owner to admin;

