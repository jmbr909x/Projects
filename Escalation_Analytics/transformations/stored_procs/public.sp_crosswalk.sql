CREATE OR REPLACE PROCEDURE public.sp_crosswalk() LANGUAGE plpgsql AS $$ BEGIN drop table if exists esc_ops.crosswalk cascade;
create table esc_ops.crosswalk diststyle auto compound sortkey ("cc_case_id", "ticket_guid", "ticket_id") as 
WITH id_join AS (
    SELECT case_id,
        esc_issue_alias
    FROM ext_src.remedy_esc_tickets
    WHERE case_id IN (
            select case_id
            from ext_src.remedy_esc_audittrail
        )
)
select distinct cast(cc.case_id as varchar(25)) as cc_case_id,
    cast(cc.ticket_id as varchar(50)) as ticket_guid,
    cast(id.esc_issue_alias as varchar(50)) as ticket_id
from ext_src.case_remedy_esc_tickets cc
    Right join id_join id on id.case_id = cc.ticket_id;
COMMIT;
END;
$$
