--DROP TABLE esc_ops.agent_skills;
CREATE TABLE IF NOT EXISTS esc_ops.agent_skills
(
	agent_login VARCHAR(25)   ENCODE RAW
	,skill_assignment VARCHAR(20)   ENCODE lzo
)
DISTSTYLE AUTO
 SORTKEY (
	agent_login
	)
;
ALTER TABLE esc_ops.agent_skills owner to admin;

