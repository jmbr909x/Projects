CREATE OR REPLACE PROCEDURE public.sp_remedy_esc_audittrail() LANGUAGE plpgsql AS $$ BEGIN drop table if exists esc_ops.remedy_esc_audittrail cascade;
create table esc_ops.remedy_esc_audittrail diststyle auto compound sortkey ("case_id", "to_string", "from_string") as
select cast(assigned_to as varchar(50)) as assigned_to,
    cast(audit_eid as varchar(100)) as audit_eid,
    cast(case_id as varchar(50)) as case_id,
    cast(created_date as timestamp) as created_date,
    cast(created_by as varchar(150)) as created_by,
    cast(description as varchar(25)) as description,
    cast(from_string as varchar(100)) as from_string,
    cast(to_string as varchar(100)) as to_string,
    cast("type" as varchar(10)) as "type",
    cast(esc_issue_guid as varchar(50)) as esc_issue_guid,
    cast(initial_case_id as varchar(50)) as initial_case_id,
    cast(parent_case_id as varchar(50)) as parent_case_id
from ext_src.remedy_esc_audittrail;
COMMIT;
END;
$$
