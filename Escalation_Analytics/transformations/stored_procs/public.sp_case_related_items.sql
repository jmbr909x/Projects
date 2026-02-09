CREATE OR REPLACE PROCEDURE public.sp_case_related_items() LANGUAGE plpgsql AS $$ BEGIN drop table if exists esc_ops.stage_case_related_items cascade;
create table esc_ops.stage_case_related_items diststyle auto compound sortkey ("case_id", "related_item_id") as with esc as (
    select case_id,
        initial_queue,
        queue
    from ext_src.case_data
),
id_join AS (
    SELECT case_id,
        esc_issue_alias
    FROM ext_src.remedy_esc_tickets
    where case_id IN (
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
esc2 as (
    select case_id,
        initial_queue,
        queue
    from ext_src.case_data
    where case_id in (
            select cc_case_id
            from esc_case
        )
)
select cast(ri.case_id as varchar(25)) as case_id,
    cast(ri.related_item_id as varchar(50)) as related_item_id,
    cast(ri.related_item_type as varchar(25)) as related_item_type,
    cast(ri.associating_agent_login_id as varchar(50)) as associating_agent_login_id,
    cast(ri.creation_date as timestamp) as creation_date,
    cast(so.initial_queue as varchar(100)) as initial_queue,
    cast(so.queue as varchar(100)) as queue
from ext_src.case_related_items ri
    inner join esc so on ri.case_id = so.case_id
union all
select cast(ri.case_id as varchar(25)) as case_id,
    cast(ri.related_item_id as varchar(50)) as related_item_id,
    cast(ri.related_item_type as varchar(25)) as related_item_type,
    cast(ri.associating_agent_login_id as varchar(50)) as associating_agent_login_id,
    cast(ri.creation_date as timestamp) as creation_date,
    cast(es2.initial_queue as varchar(100)) as initial_queue,
    cast(es2.queue as varchar(100)) as queue
from ext_src.case_related_items ri
    inner join esc2 es2 on ri.case_id = es2.case_id;
COMMIT;
drop table if exists esc_ops.case_related_items cascade;
create table esc_ops.case_related_items diststyle auto compound sortkey ("case_id", "related_item_id") as
select distinct *
from esc_ops.stage_case_related_items
order by case_id;
COMMIT;
drop table if exists esc_ops.stage_case_related_items cascade;
COMMIT;
END;
$$
