--DROP VIEW public.vw_ticket_time_snapshot;
CREATE OR REPLACE VIEW public.vw_ticket_time_snapshot AS



create or replace view vw_ticket_time_snapshot

as

with pool as (select st_esc_ticket_id,  current_ticket_owner, st_esc_ticket_aging_raw, st_esc_ticket_resolved_date  
from public.vw_health_workload_detail_final 
where current_ticket_owner is not null 
	and st_esc_ticket_status in ('Resolved','Closed'))

, st as (select sim_issue_alias, 'ST Ownership' as owner, sum(days_with_owning_team) as st_time_adj
from public.vw_ticket_group_assignment_breakout_linked
where owning_team != 'AWS Support Operations - RS'
group by 1,2)


,so as (select sim_issue_alias, 'Support Ops Ownership' as owner, sum(days_with_owning_team) as so_time_adj
from public.vw_ticket_group_assignment_breakout_linked
where owning_team = 'AWS Support Operations - RS'
group by 1,2)

,build as (
select concat(date_part('month',st_esc_ticket_resolved_date),concat('-01-',date_part('year',st_esc_ticket_resolved_date))) as time_period
, case when current_ticket_owner = 'AWS Support Operations - RS' then 'AWS Support Operations - RS' else 'Service Team' end as ticket_owner
, count(st_esc_ticket_id) as ticket_ct
, sum(st_esc_ticket_aging_raw) as total_ticket_time_raw
, sum(adj_owner_time) as total_adj_ticket_time
, sum(so_time_adj) as so_add_time
, sum(st_time_adj) as st_add_time

from  (
--*/
select p.*
, case when current_ticket_owner = 'AWS Support Operations - RS' then st_esc_ticket_aging_raw - isnull(st.st_time_adj,0) else
  st_esc_ticket_aging_raw - isnull(so.so_time_adj,0) end as adj_owner_time
, case when current_ticket_owner = 'AWS Support Operations - RS' then isnull(st.st_time_adj,0) else 0 end as st_time_adj
, case when current_ticket_owner != 'AWS Support Operations - RS' then isnull(so.so_time_adj,0) else 0 end as so_time_adj
from pool p
left join st on p.st_esc_ticket_id = st.sim_issue_alias
left join so on p.st_esc_ticket_id = so.sim_issue_alias
--/*
)

group by 1,2
order by 1,2)



select l1.time_period
	, l1.ticket_owner
	, l1.ticket_ct
	, round(l1.total_ticket_time_raw/l1.ticket_ct,2) as raw_ticket_average_time
	, round(case when l1.ticket_owner = 'AWS Support Operations - RS' then (l1.total_adj_ticket_time + l2.so_add_time)/l1.ticket_ct 
		else (l1.total_adj_ticket_time + l2.st_add_time)/l1.ticket_ct end,2) as adj_ticket_average_time
from (select time_period, ticket_owner, ticket_ct, total_ticket_time_raw, total_adj_ticket_time
  		from build) l1
inner join (select time_period,  case when ticket_owner = 'AWS Support Operations - RS' then 'Service Team' else 'AWS Support Operations - RS' end as ticket_owner
			, sum(so_add_time) as so_add_time
			, sum(st_add_time) as st_add_time
		from build group by 1,2) l2 on l1.time_period = l2.time_period and l1.ticket_owner = l2.ticket_owner
with no schema binding

;



