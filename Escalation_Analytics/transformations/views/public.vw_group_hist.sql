--DROP VIEW public.vw_group_hist;

CREATE OR REPLACE VIEW public.vw_group_hist as
with ordered_changes as (
select ticket_id
		, created_date
		, lag(to_string) over (partition by ticket_id order by created_date) as prev_owner
		, to_string as current_owner
		, from_string as from_owner
		, created_date as change_time
	from (select distinct
	cr.ticket_id,
	cast(aut.created_date as timestamp) as created_date,
	aut.description,
	aut.from_string,
	aut.to_string
from
	(
	select
		case_id,
		created_date,
		description,
		from_string,
		to_string
	from
		esc_ops.remedy_esc_audittrail) aut
inner join esc_ops.crosswalk cr on
	cr.ticket_guid = aut.case_id
where
	description = 'Assigned Group'
	and case_id in (
	select
		case_id
	from
		esc_ops.remedy_esc_audittrail au
	inner join esc_ops.crosswalk cr on
		cr.ticket_guid = au.case_id
	where
		description = 'Status'
		and to_string = 'Assigned'))
where isnull(to_string,'') != ''
)

, grouped_changes as (
select ticket_id
		, current_owner
		, change_time
		, sum(case when current_owner <> prev_owner or prev_owner is null then 1 else 0 end) over (partition by ticket_id order by change_time rows unbounded preceding) as owner_group
	from ordered_changes
)
, timeline_blocks as (
select ticket_id
		, current_owner as owning_team
		, min(change_time) as start_time
		, lead(min(change_time)) over (partition by ticket_id order by min(change_time)) as next_start_time
	from grouped_changes
	group by ticket_id, current_owner, owner_group
)
,ticket_detail as (
	select sim_issue_alias as ticket_id
		, status
		, assigned_to_group as cur_assigned_ticket_grp
		, cast(create_date as timestamp) as ticket_creation_date
		, cast(case when resolved_date is null then '1900-01-01 00:00:00.000' else resolved_date end as timestamp) as ticket_resolved_date
		, getdate() as ticket_reporting_date
		from esc_ops.remedy_esc_tickets
		)

,ticket_group_assn as
(select rn
		,ticket_id
		, 'Assigned Group' as audit_action
		,created_date
		,start_time as datetime_start
		,end_time as datetime_end
		,from_string as prior_owner
		,to_string as owning_target
	from (select row_number() over (partition by ticket_id order by created_date) as rn, 
*
from (
select g.ticket_id
		, a. created_date
		, g.start_time
		, g.end_time
		, a.from_string
		, a.to_string
from (

select ticket_id
		, owning_team
		, start_time
		, coalesce(next_start_time, getdate()) as end_time
	from timeline_blocks
	order by ticket_id, start_time) g
join (select distinct
	cr.ticket_id,
	cast(aut.created_date as timestamp) as created_date,
	aut.description,
	aut.from_string,
	aut.to_string
from
	(
	select
		case_id,
		created_date,
		description,
		from_string,
		to_string
	from
		esc_ops.remedy_esc_audittrail) aut
inner join esc_ops.crosswalk cr on
	cr.ticket_guid = aut.case_id
where
	description = 'Assigned Group'
	and case_id in (
	select
		case_id
	from
		esc_ops.remedy_esc_audittrail au
	inner join esc_ops.crosswalk cr on
		cr.ticket_guid = au.case_id
	where
		description = 'Status'
		and to_string = 'Assigned')) a on g.ticket_id = a.ticket_id and g.owning_team = a.to_string and g.start_time = a.created_date
order by g.ticket_id, g.start_time)) a 
	order by 4 asc)
	

	
, ticket_group_assn_stage as (
select st.ticket_id
	, st.rn as grp_rn
	, det.cur_assigned_ticket_grp
	, det.status
	, det.ticket_creation_date
	, det.ticket_resolved_date
	, datetime_start
	, st.datetime_end
	, st.prior_owner
	, st.owning_target  
	from ticket_group_assn st	
inner join ticket_detail det on det.ticket_id = st.ticket_id)


,group_agg as (
select distinct grp_rn 
		,ticket_id
		,cur_assigned_ticket_grp
		,status
		,ticket_creation_date
		,ticket_resolved_date
		,prior_owner
		,owning_target
		,datetime_start
		,datetime_end	
from ticket_group_assn_stage

)

select * from group_agg
with no schema binding

;






