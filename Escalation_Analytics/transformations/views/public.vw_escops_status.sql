--DROP VIEW public.vw_escops_status;
CREATE OR REPLACE VIEW public.vw_escops_status AS
-- public.vw_escops_status source

create or replace view vw_escops_status as 
select det.*, dist.Support_Ops_Owned, dist.OtherTeam_Owned from
(select distinct cast(listagg(distinct child_case_owner, ', ') within group (order by child_case_id) over ( partition by parent_case_id) as varchar(500)) as child_case_owner
,cast(listagg(distinct child_case_id, ', ') within group (order by child_case_id) over ( partition by parent_case_id) as varchar(500))  as child_case_id
,parent_case_owner
,parent_case_id
,st_esc_ticket_id
,st_esc_ticket_create_date
,total_bounce_ct
,pcase_id
,pcase_status
,ticket_id
,st_esc_ticket_status 
from (select  distinct soeng_assigned_owner as child_case_owner
				,so.soesc_case_id as child_case_id
				,so.parent_case_owner
				,so.parent_case_id
				,so.st_escalation_ticket_id as st_esc_ticket_id
				,tt.create_date as st_esc_ticket_create_date
				,sum(tt.total_bounce_ct) as total_bounce_ct
				,so.parent_case_id as pcase_id
				,status as pcase_status
				,so.st_escalation_ticket_id as ticket_id
				,tt.ticket_status as st_esc_ticket_status
from public.vw_ticket_group_assignment_breakout_linked tt
inner join public.vw_ticket_links so on so.parent_case_id = tt.parent_case_id and so.st_escalation_ticket_id = tt.sim_issue_alias
group by 1,2,3,4,5,6,8,9,10,11
)) det
inner join (select sim_issue_alias
		, round(max(running_ticket_days),1) as total_ticket_days
		, round(sum(case when owning_team = 'AWS Support Operations - RS' then days_with_owning_team else 0 end),1) as Support_Ops_Owned
		, round(sum(case when owning_team != 'AWS Support Operations - RS' then days_with_owning_team else 0 end),1) as OtherTeam_Owned
	from public.vw_ticket_group_assignment_breakout_linked
	group by sim_issue_alias) dist on det.st_esc_ticket_id = dist.sim_issue_alias
with no schema binding;




