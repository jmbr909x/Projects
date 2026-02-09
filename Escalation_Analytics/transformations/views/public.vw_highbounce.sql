--DROP VIEW public.vw_highbounce;
create or replace view vw_highbounce as
select distinct listagg(distinct child_case_owner, ', ') within group (order by child_case_id) over ( partition by parent_case_id) as child_case_owner
,listagg(distinct child_case_id, ', ') within group (order by child_case_id) over ( partition by parent_case_id) as child_case_id
,parent_case_owner
,parent_case_id
,esc_ticket_id
,esc_ticket_create_date
,total_bounce_ct
,pcase_id
,pcase_status
,ticket_id
,esc_ticket_status 
from (select  distinct soeng_assigned_owner as child_case_owner
				,so.soesc_case_id as child_case_id
				,so.parent_case_owner
				,so.parent_case_id
				,so.escalation_ticket_id as esc_ticket_id
				,tt.create_date as esc_ticket_create_date
				,sum(tt.total_bounce_ct) as total_bounce_ct
				,so.parent_case_id as pcase_id
				,status as pcase_status
				,so.escalation_ticket_id as ticket_id
				,tt.status as esc_ticket_status
from public.vw_ticket_group_assignment_breakout_linked tt
inner join public.vw_ticket_links so on so.parent_case_id = tt.parent_case_id and so.escalation_ticket_id = tt.sim_issue_alias
group by 1,2,3,4,5,6,8,9,10,11
)		
order by total_bounce_ct desc
with no schema binding
	;



