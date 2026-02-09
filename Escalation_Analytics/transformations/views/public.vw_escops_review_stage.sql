--DROP VIEW public.vw_escops_review_stage;
CREATE OR REPLACE VIEW public.vw_escops_review_stage AS

create view vw_escops_review_stage as
select distinct create_date
	, sim_issue_alias
	, 'https://t.corp.amazon.com/'+ sim_issue_alias as link
	,'' as due_dilligence_met
	, case_id 
	, '' as Inescopsquip
	, requester_login
	, '' as title 
	, '' as dept
	, assigned_to_group 
	, assigned_to_individual 
	, status 
	, root_cause 
	, root_cause_details 
	, short_description 
from
	ext_src.remedy_esc_tickets
where
	case_id in (
		select
			distinct case_id
		from
			ext_src.remedy_esc_audittrail
		where
			description = 'Assigned Group'
			and to_string ilike '%AWS Support Operations - RS%'
	)
	order by create_date asc
with no schema binding;






