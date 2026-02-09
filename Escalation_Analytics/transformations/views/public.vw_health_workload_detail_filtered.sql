--DROP VIEW public.vw_health_workload_detail_filtered;
create or replace view vw_health_workload_detail_filtered as
select distinct soesc_case_id
,esc_owner
,CFX
,esc_own_date
,date_x2
,esc_assign_order
,parent_case_id
,pcase_owner
,pcase_own_date
,pcase_assign_order
,pcase_status
,pcase_creation_date
,soesc_date
,pcase_status_date
,parent_case_days
,soesc_days
,esc_ticket_id
,ticket_timing
,time_to_esc_days
,esc_ticket_requestor
,esc_ticket_group
,esc_ticket_assignee
,isnull(esc_ticket_status,'') as esc_ticket_status
,esc_ticket_create_date
,esc_ticket_modified_date
,esc_ticket_resolved_date
,esc_ticket_aging_raw
,current_ticket_owner
,total_bounce_ct
,eng_alias
,site
,manager
from public.vw_health_workload_detail det
left join esc_ops.service_sme sme on sme.eng_alias = det.pcase_owner
with no schema binding	;





