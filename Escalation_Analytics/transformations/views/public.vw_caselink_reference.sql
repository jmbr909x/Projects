--DROP VIEW public.vw_caselink_reference;
create or replace view vw_caselink_reference as
select distinct cast(parent_case_id as varchar(25)) as parent_case_id
	,cast(pcase_owner as varchar(25)) as pcase_owner
	,st_esc_ticket_id
	,'https://command-center.support.aws.a2z.com/case-console?CSGroupId=1b957cb7-c42c-4f3c-8730-797c2a5fdf5c#/cases/'+parent_case_id as case_link
	,case when isnull(st_esc_ticket_id,'')='' then '' else 'https://t.corp.amazon.com/'+st_esc_ticket_id end as ticket_link
from public.vw_health_workload_detail_filtered 
with no schema binding;



