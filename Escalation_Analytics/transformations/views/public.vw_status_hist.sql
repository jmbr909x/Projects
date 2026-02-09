--DROP VIEW public.vw_status_hist;
CREATE OR REPLACE VIEW public.vw_status_hist as
with ticket_detail as (
	select esc_issue_alias as ticket_id
		, status
		, assigned_to_group as cur_assigned_ticket_grp
		, cast(create_date as timestamp) as ticket_creation_date
		, cast(case when resolved_date is null then '1900-01-01 00:00:00.000' else resolved_date end as timestamp) as ticket_resolved_date
		, getdate() as ticket_reporting_date 
		from esc_ops.remedy_esc_tickets
		)
, ticket_status as 
(select description as audit_action
		, ticket_id 
		, cast(case when lag(created_date) over (partition by ticket_id order by datetime_end asc) is null 
			then '1900-01-01 00:00:00' 
			else lag(created_date) over (partition by ticket_id order by datetime_end asc) end as timestamp) as datetime_start
		, cast(created_date as timestamp) as datetime_end
		, from_string as owning_target
		, to_string as future_target
from (select cr.ticket_id,
		created_date,
		description,
		from_string,
		to_string
from esc_ops.remedy_esc_audittrail au
inner join esc_ops.crosswalk cr on cr.ticket_guid = au.case_id
	where description = 'Status' and to_string <> 'Closed' and case_id in 
		(select case_id 
			from esc_ops.remedy_esc_audittrail au
			inner join esc_ops.crosswalk cr on cr.ticket_guid = au.case_id
			where description = 'Status' and to_string = 'Assigned') ) 
where from_string = 'Resolved' or to_string = 'Resolved' and 
	isnull(from_string,'') != '' 
)
		
, ticket_status_stage as (
 select st.ticket_id
 	, row_number() over (partition by st.ticket_id order by datetime_end asc) as status_rn
	, det.cur_assigned_ticket_grp
	, det.status
	, case when st.datetime_start = '1900-01-01 00:00:00' then det.ticket_creation_date else st.datetime_start end as datetime_start
	, st.datetime_end
	, st.owning_target
	, st.future_target 
from ticket_status st
inner join ticket_detail det on det.ticket_id = st.ticket_id)



, res_status_stage as (
select  row_number() over (partition by ticket_id order by datetime_end) as rn
		,ticket_id 
		, cur_assigned_ticket_grp 
		, status
		, datetime_start
		, datetime_end
		, owning_target
		, future_target
		, 0 as current_flag
from ticket_status_stage
)


, final_status_stage as (
select   st.rn + 1
		, st.ticket_id 
		, st.cur_assigned_ticket_grp 
		, st.status
		, st.datetime_end as datetime_start
		, getdate() as datetime_end
		, st.future_target as owning_target
		, '' as future_target
		, 1 as current_flag
from res_status_stage st
inner join (select ticket_id, max(rn) as rn from res_status_stage group by ticket_id) mx on mx.ticket_id = st.ticket_id and mx.rn = st.rn
)


, res_status as (
select ticket_id
		,cur_assigned_ticket_grp
		,status
		,datetime_start
		,datetime_end
		,owning_target
		,future_target 
	from (select * from res_status_stage
			union all
		  select * from final_status_stage
			) 
where (owning_target = 'Resolved' or current_flag = 1)
order by ticket_id, rn)

select * from res_status
with no schema binding;






