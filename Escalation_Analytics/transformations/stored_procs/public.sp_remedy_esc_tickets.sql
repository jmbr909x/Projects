CREATE OR REPLACE PROCEDURE public.sp_remedy_esc_tickets() LANGUAGE plpgsql AS $$ BEGIN drop table if exists esc_ops.stage_remedy_esc_tickets cascade;
create table esc_ops.stage_remedy_esc_tickets diststyle auto compound sortkey ("case_id", "create_date", "assigned_to_group") 
as 
select cast(create_date as timestamp) as create_date,
cast(case_id as varchar(50)) as case_id,
cast(assigned_to_group as varchar(50)) as assigned_to_group,
cast(assigned_to_individual as varchar(50)) as assigned_to_individual,
cast(assignee_manager_login as varchar(25)) as assignee_manager_login,
cast(department as varchar(50)) as department,
cast(impact_label as varchar(50)) as impact_label,
cast(item as varchar(100)) as item,
cast(modified_date as timestamp) as modified_date,
cast(requester_login as varchar(150)) as requester_login,
cast(resolved_date as timestamp) as resolved_date,
cast(resolved_by as varchar(150)) as resolved_by,
cast(status as varchar(25)) as status,
cast(root_cause as varchar(100)) as root_cause,
cast(root_cause_details as varchar(500)) as root_cause_details,
cast(submitted_by as varchar(50)) as submitted_by,
cast("type" as varchar(50)) as "type",
cast(esc_issue_alias as varchar(50)) as esc_issue_alias,
cast(short_description as varchar(500)) as short_description
from ext_src.remedy_esc_tickets;
COMMIT;
drop table if exists esc_ops.remedy_esc_tickets cascade;

create table esc_ops.remedy_esc_tickets diststyle auto compound sortkey ("case_id", "create_date", "assigned_to_group") as 
with src as (
    select distinct create_date,
        case_id,
        assigned_to_group,
        assigned_to_individual,
        assignee_manager_login,
        department,
        impact_label,
        item,
        modified_date,
        requester_login,
        resolved_date,
        resolved_by,
        status,
        root_cause,
        root_cause_details,
        submitted_by,
        type,
        esc_issue_alias,
        short_description
    from esc_ops.stage_remedy_esc_tickets
    order by case_id,
        modified_Date asc
),
src2 as (
    select row_number() over (
            partition by case_id
            order by modified_date asc
        ) as rn,
        *
    from src
),
dedup as (
    select case_id,
        max(rn) as rn
    from src2
    group by case_id
)
select s.*
from src2 s
    inner join dedup d on s.case_id = d.case_id
    and s.rn = d.rn;
COMMIT;
drop table if exists esc_ops.stage_remedy_esc_tickets cascade;
COMMIT;
END;
$$
