--DROP VIEW public.vw_health_workload_detail;
CREATE OR REPLACE VIEW public.vw_health_workload_detail AS

with grp_st_merge as
(select distinct * from
(select grp.ticket_id
		,grp.cur_assigned_ticket_grp
		,grp.current_ticket_status
		,grp.ticket_creation_date as create_date
		,grp.ticket_resolved_date as resolved_date
		,grp.owning_target	as owning_team	
		,grp.prior_owner as previous_owning_team
		,grp.datetime_start
		,grp.datetime_end
		,st.owning_target as owning_status
		,st.datetime_start as status_start
		,st.datetime_end as status_end
from (
select ticket_id
		,cur_assigned_ticket_grp
		,status as current_ticket_status
		,ticket_creation_date
		,ticket_resolved_date
		,owning_target
		,prior_owner
		,datetime_start
		,datetime_end
from public.vw_group_hist 
) grp
left join(
select ticket_id
		,cur_assigned_ticket_grp
		,status as current_ticket_status
		,owning_target
		,future_target
		,datetime_start
		,datetime_end 
from public.vw_status_hist
where owning_target = 'Resolved' 
) st on grp.ticket_id = st.ticket_id 
	and st.datetime_start between grp.datetime_start and grp.datetime_end 
	and st.datetime_end between grp.datetime_start and grp.datetime_end
	) 
order by ticket_id, datetime_end)


select distinct com.soesc_case_id
		,com.esc_owner
		,com.CFX
		,com.esc_own_date
		,cast(left(com.esc_own_date,10) as timestamp) as date_x2
		,com.esc_assign_order
		,com.parent_case_id
		,com.parent_case_owner as pcase_owner
		,com.pcase_own_date
		,com.pcase_assign_order
		,com.pcase_status
		,com.pcase_creation_date
		,com.soesc_date
		,com.pcase_status_date
		,com.parent_case_days
		,com.soesc_days
		,com.escalation_ticket_id as esc_ticket_id
		,com.ticket_timing
		,case when com.ticket_timing = 'Ticket predates escalation' then 0 else com.time_to_esc_days end as time_to_esc_days
		,com.esc_ticket_requestor
		,com.esc_ticket_group
		,com.esc_ticket_assignee
		,com.esc_ticket_status
		,com.esc_ticket_create_date
		,com.esc_ticket_modified_date
		,com.esc_ticket_resolved_date
		,com.esc_ticket_aging_raw
		,com.current_ticket_owner
		,com.total_bounce_ct
from (select distinct com.soesc_case_id
		,com.esc_owner
		,case when an.case_id is null then 0 else 1 end as CFX
		,com.esc_own_date
		,com.esc_assign_order
		,com.parent_case_id
		,com.parent_case_owner
		,com.pcase_own_date
		,com.pcase_assign_order
		,com.pcase_status
		,com.pcase_creation_date
		,com.soesc_date
		,com.pcase_status_date
		,case when com.pcase_status is null then null else round(cast(datediff("minutes",com.pcase_creation_date,com.pcase_status_date)/1440.00 as decimal (10,4)),2) end as parent_case_days
		,case when com.pcase_status is null then null else round(cast(datediff("minutes",cast(com.soesc_date as timestamp),com.pcase_status_date)/1440.00 as decimal (10,4)),2) end as soesc_days
		,com.escalation_ticket_id
		,case when (case when isnull(com.escalation_ticket_id,'') ='' then null 
		else round(cast(datediff("minutes",cast(com.soesc_date as timestamp),cast(com.esc_ticket_create_date as timestamp))/1440.00 as decimal (10,4)),2) end)< 0 then 'Ticket predates escalation' else '' end ::varchar(50) as ticket_timing
		,case when isnull(com.escalation_ticket_id,'') ='' then null else round(cast(datediff("minutes",cast(com.soesc_date as timestamp),cast(com.esc_ticket_create_date as timestamp))/1440.00 as decimal (10,4)),2) end as time_to_esc_days
		,com.esc_ticket_requestor
		,com.esc_ticket_group
		,com.esc_ticket_assignee
		,com.esc_ticket_status
		,com.esc_ticket_create_date
		,com.esc_ticket_modified_date
		,com.esc_ticket_resolved_date
		,com.esc_ticket_aging_raw
		,com.current_ticket_owner
		,tbc.total_bounce_count as total_bounce_ct
from public.vw_combined_wl_e2e com
left join (select * from esc_ops.case_annotation where replace(created_by,'"','') = 'CFX') an on com.soesc_case_id = an.case_id
left join (select ticket_id, count(ticket_id) as total_bounce_count from (
Select row_number() over (partition by ticket_id order by datetime_end) as rn
        ,ticket_id
        ,datetime_start
        ,datetime_end
        ,'Assigned Group' as description
        ,previous_owning_team as from_string
        ,owning_team as to_string
        ,owning_team
        ,cast(datediff(minute,cast(datetime_start as timestamp), cast(datetime_end as timestamp))/1440.00 as decimal(10,4))
            as days_with_owning_team
        ,current_ticket_status as status
        ,cur_assigned_ticket_grp
        ,create_date
        ,resolved_date
        , 1 as bounce_flag
from (select * from grp_st_merge)
order by ticket_id, datetime_start, datetime_end
) group by ticket_id) tbc on tbc.ticket_id = com.escalation_ticket_id
where isnull(pcase_status,'') != ''
order by parent_case_id,esc_assign_order, pcase_assign_order) com
where isnull(pcase_status,'') != ''
order by parent_case_id,esc_assign_order, pcase_assign_order
with no schema binding	;





