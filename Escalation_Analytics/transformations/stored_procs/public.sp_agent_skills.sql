CREATE OR REPLACE PROCEDURE public.sp_agent_skills() LANGUAGE plpgsql AS $$ BEGIN drop table if exists esc_ops.agent_skills cascade;
create table esc_ops.agent_skills diststyle auto compound sortkey ("agent_login") as with soesc as (
    select case_id,
        initial_queue,
        queue
    from ext_src.case_data
),
id_join AS (
    SELECT case_id,
        esc_issue_alias
    FROM ext_src.remedy_esc_tickets
    WHERE case_id IN (
            select distinct case_id
            from ext_src.remedy_esc_audittrail
        )
),
esc_case as (
    select cc.case_id as cc_case_id,
        cc.ticket_id as ticket_guid,
        id.esc_issue_alias as ticket_id
    from ext_src.case_remedy_esc_tickets cc
        Right join id_join id on id.case_id = cc.ticket_id
),
soesc2 as (
    select case_id,
        initial_queue,
        queue
    from ext_src.case_data
    where case_id in (
            select cc_case_id
            from esc_case
        )
),
case_check as (
    select case_id as cc_case_id
    from soesc
    union all
    select case_id as cc_case_id
    from soesc2
)
select distinct cast(assigned_agent_login as varchar(25)) as agent_login,
    case
        when initial_queue in (
            '"support-tier1"',
            '"support-tier2"',
            '"support-tier3"'
        ) then 'service-frontline'
        when initial_queue = '"EscOps"' then 'Escalation-ops'
        when initial_queue = '"guidance"' then 'onboarding'
        else ''
    end as skill_assignment
from ext_src.case_data cd
where case_id in (
        select cc_case_id
        from case_check
    );
COMMIT;
END;
$$
