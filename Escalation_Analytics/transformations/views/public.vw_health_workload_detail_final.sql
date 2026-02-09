--DROP VIEW public.vw_health_workload_detail_final;

create or replace view vw_health_workload_detail_final as
with scope as (SELECT cr.ticket_id, max(created_date) as created_date
FROM esc_ops.remedy_esc_audittrail at 
inner join esc_ops.crosswalk cr on cr.ticket_guid = at.case_id
where description = 'Root Cause' and to_string in ('RCA1',
'RCA2',
'RCA3',
'RCA4',
'RCA5',
'RCA6',
'RCA7',
'RCA8',
'RCA9',
'RCA10') group by cr.ticket_id)
, drill_down as (
SELECT cr.cc_case_id, cr.ticket_id, at.created_by, at.to_string, at.created_date
FROM esc_ops.remedy_esc_audittrail at 
inner join esc_ops.crosswalk cr on cr.ticket_guid = at.case_id
inner join scope on scope.ticket_id = cr.ticket_id and scope.created_date = at.created_date
where description = 'Root Cause' and to_string in ('RCA1',
'RCA2',
'RCA3',
'RCA4',
'RCA5',
'RCA6',
'RCA7',
'RCA8',
'RCA9',
'RCA10')
order by 2)
select hwd.*
,dd.to_string as resolve_reason
from public.vw_health_workload_detail_filtered hwd
left join drill_down dd on hwd.st_esc_ticket_id = dd.ticket_id
with no schema binding
;






