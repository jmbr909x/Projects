--DROP VIEW public.vw_ticket_group_assignment_breakout_linked;

CREATE OR REPLACE VIEW public.vw_ticket_group_assignment_breakout_linked
AS 

with max_rn as (
select esc_issue_alias, max(rn) as rn from (select
	distinct l.parent_case_id
	, l.pcase_owner
	, l.pcase_status
	, tt.esc_issue_alias
	, row_number() over (partition by esc_issue_alias order by grpstart_dt) as rn
	, tt.grpstart_dt
	, tt.grpend_dt
	, tt.owning_team
	, tt.datetime_start as anchor_date
	, tt.status
	, tt.cur_assigned_ticket_grp as assigned_to_group
	, tt.ticket_status
	, tt.create_date
	, tt.modified_date
	, tt.ticket_resolved_date
	, tt.ticket_create_date
	, tt.ticket_modified_date
	, 1 as total_bounce_ct
from
	(select distinct *
 from (
Select Distinct
		ag.ticket_id as esc_issue_alias
		,cast(ag.datetime_start as timestamp) as GrpStart_dt
		,cast(ag.datetime_end as timestamp) as GrpEnd_dt
		,ag.owning_team
		,ag.days_with_owning_team
		,ag.datetime_start
		,ag.status
		,ag.cur_assigned_ticket_grp
		,ti.status as ticket_status
		,ti.create_date
		,ti.modified_date
		,cast(case when ti.resolved_date is null then '1900-01-01 00:00:00' else ti.resolved_date end as timestamp) as ticket_resolved_date
		,cast(ti.create_date as timestamp) as ticket_create_date
		,cast(isnull(ti.modified_date,'2999-01-01 00:00:00') as timestamp) as ticket_modified_date
from public.vw_ticket_aging_detail ag
inner join esc_ops.remedy_esc_tickets ti on ag.ticket_id = ti.esc_issue_alias
order by 1,2)) tt
inner join (
	select
		distinct parent_case_id,
		pcase_owner,
		pcase_status,
		st_esc_ticket_id,
		total_bounce_ct
	from
		public.vw_health_workload_detail
	where
		isnull(st_esc_ticket_id, '') != ''
		) l on
		tt.esc_issue_alias = l.st_esc_ticket_id) group by esc_issue_alias )

,term_build as (
select tt.*, case when mx.esc_issue_alias is not null then 1 else 0 end as lflag
from (select
	distinct l.parent_case_id
	, l.pcase_owner
	, l.pcase_status
	, tt.esc_issue_alias
	, row_number() over (partition by esc_issue_alias order by grpstart_dt) as rn
	, tt.grpstart_dt
	, tt.grpend_dt
	, tt.owning_team
	, tt.datetime_start as anchor_date
	, tt.status
	, tt.cur_assigned_ticket_grp as assigned_to_group
	, tt.ticket_status
	, tt.create_date
	, tt.modified_date
	, tt.ticket_resolved_date
	, tt.ticket_create_date
	, tt.ticket_modified_date
	, 1 as total_bounce_ct
from
	(select distinct *
 from (
Select Distinct
		ag.ticket_id as esc_issue_alias
		,cast(ag.datetime_start as timestamp) as GrpStart_dt
		,cast(ag.datetime_end as timestamp) as GrpEnd_dt
		,ag.owning_team
		,ag.days_with_owning_team
		,ag.datetime_start
		,ag.status
		,ag.cur_assigned_ticket_grp
		,ti.status as ticket_status
		,ti.create_date
		,ti.modified_date
		,cast(case when ti.resolved_date is null then '1900-01-01 00:00:00' else ti.resolved_date end as timestamp) as ticket_resolved_date
		,cast(ti.create_date as timestamp) as ticket_create_date
		,cast(isnull(ti.modified_date,'2999-01-01 00:00:00') as timestamp) as ticket_modified_date
from public.vw_ticket_aging_detail ag
inner join esc_ops.remedy_esc_tickets ti on ag.ticket_id = ti.esc_issue_alias
order by 1,2)) tt
inner join (
	select
		distinct parent_case_id,
		pcase_owner,
		pcase_status,
		st_esc_ticket_id,
		total_bounce_ct
	from
		public.vw_health_workload_detail
	where
		isnull(st_esc_ticket_id, '') != ''
		) l on
		tt.esc_issue_alias = l.st_esc_ticket_id) tt
left join max_rn mx on tt.esc_issue_alias = mx.esc_issue_alias and tt.rn = mx.rn) 

, flag_date as (
select parent_case_id
,pcase_owner
,pcase_status
,esc_issue_alias
,rn
,grpstart_dt
,case when status in ('Closed','Resolved') and lFlag = 1 then ticket_resolved_date else grpend_dt end as grpend_dt
,owning_team
,anchor_date
,status
,assigned_to_group
,ticket_status
,create_date
,modified_date
,ticket_resolved_date
,ticket_create_date
,ticket_modified_date
,total_bounce_ct
from term_build)

select parent_case_id
,pcase_owner
,esc_issue_alias
,rn
,grpstart_dt
,grpend_dt
,owning_team
,round(datediff(minutes, grpstart_dt, grpend_dt)/1440.00,2) as days_with_owning_team
,anchor_date
,pcase_status as status
,assigned_to_group
,ticket_status
,create_date
,modified_date
,ticket_resolved_date
,ticket_create_date
,ticket_modified_date
, round(sum(datediff(minutes, grpstart_dt, grpend_dt)/1440.00) over (partition by esc_issue_alias order by grpstart_dt rows unbounded preceding),2) as running_ticket_days
, round(case when status in ('Resolved','Closed') then cast(datediff('minutes',ticket_create_date,ticket_resolved_date)/1440.00 as decimal(10,2))
	when status not in ('Resolved','Closed') then cast(datediff('minutes',ticket_create_date,getdate())/1440.00 as decimal(10,2))
	else 0 end,2) as total_ticket_days_holistic
,total_bounce_ct 
from flag_date
with no schema binding;






