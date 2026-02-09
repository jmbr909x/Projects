--DROP VIEW public.vw_combined_wl_e2e;

CREATE OR REPLACE VIEW public.vw_combined_wl_e2e
AS

with own_add as (
select distinct cd.case_id
		,cd.operation
		,cd.assigned_agent_login
		,min(cd.message_timestamp) as ownership_date
		,row_number() over (partition by case_id order by ownership_date) as assignee_num
from esc_ops.case_data cd
left join (select *
		from esc_ops.agent_skills
		where skill_assignment = 'Escalation-ops'
	) so on cd.assigned_agent_login = so.agent_login
where operation in ('AddCaseAgent')
group by 1,2,3)

select distinct soesc_case_id
		,esc.esc_owner
		,esc.esc_own_date
		,esc.esc_assign_order
		,esc.parent_case_id
		,esc.parent_case_owner
		,esc.pcase_own_date
		,esc.pcase_assign_order
		,esc.pcase_status
		,esc.pcase_creation_date
		,esc.soesc_date
		,esc.pcase_status_date
		,esc.ticket_requestor as esc_ticket_requestor
		,esc.escalation_ticket_id
		,esc.ticket_group as esc_ticket_group
		,esc.ticket_assignee as esc_ticket_assignee
		,esc.ticket_status as esc_ticket_status
		,esc.ticket_create_date as esc_ticket_create_date
		,esc.ticket_modified_date as esc_ticket_modified_date
		,esc.ticket_resolved_date as esc_ticket_resolved_date
		,esc.ticket_aging_raw as esc_ticket_aging_raw
		,esc.current_ticket_owner as current_ticket_owner
from (select distinct soesc_case_id,
	esc.esc_owner,
	esc.esc_own_date,
	esc.esc_assign_order,
	esc.parent_case_id,
	esc.parent_case_owner,
	oa2.ownership_date as pcase_own_date,
	oa2.assignee_num as pcase_assign_order,
	esc.pcase_status,
	esc.pcase_creation_date,
	esc.soesc_date,
	esc.pcase_status_date,
	esc.ticket_requestor,
	esc.escalation_ticket_id,
	esc.ticket_group,
	esc.ticket_assignee,
	esc.ticket_status,
	esc.ticket_create_date,
	esc.ticket_modified_date,
	esc.ticket_resolved_date,
	esc.ticket_aging_raw,
	esc.current_ticket_owner
from (
select distinct soesc_case_id
		,oa1.assigned_agent_login as esc_owner
		,oa1.ownership_date as esc_own_date
		,oa1.assignee_num as esc_assign_order
		,esc.parent_case_id
		,esc.parent_case_owner
		,esc.parent_case_status as pcase_status
		,esc.parent_case_creation_date as pcase_creation_date
		,esc.soesc_date as soesc_date
		,esc.parent_case_status_date as pcase_status_date
		,esc.ticket_requestor
		,esc.escalation_ticket_id
		,esc.ticket_group
		,esc.ticket_assignee
		,esc.ticket_status
		,esc.ticket_create_date
		,esc.ticket_modified_date
		,esc.ticket_resolved_date
		,esc.ticket_aging_raw

		,esc.current_ticket_owner
from (select *
from (select cl.soesc_case_id
	,cl.primary_escops_eng
	,cl.parent_case_id
	,cl.parent_case_status
	,cl.parent_case_creation_date
	,cl.soesc_date
	,cl.parent_case_status_date
	,cl.parent_case_owner
	,cl.escalation_ticket_id
	,cl.ticket_group
	,cl.ticket_assignee
	,cl.ticket_status
	,cl.ticket_create_date
	,cl.ticket_modified_date
	,cl.ticket_resolved_date
	,cl.ticket_requestor
	,cl.ticket_aging_raw
from (select soesc_case_id
		,soesc_date
		,primary_escops_eng
		,parent_case_id
		,parent_case_status
		,parent_case_creation_date
		,parent_case_status_date
		,parent_case_owner
		,escalation_ticket_id
		,ticket_group
		,ticket_assignee
		,ticket_status
		,ticket_create_date
		,ticket_modified_date
		,ticket_resolved_date
		,ticket_requestor
		,ticket_aging_raw
from
	(select cs.soesc_case_id
	, cs.creation_date as soesc_date
	, cs.soeng_assigned_owner as primary_escops_eng
	, '' as primary_escops_eng_site
	, cs.parent_case_id
	, sts.status as parent_case_status
	, sts.parent_case_creation_date
	, sts.status_date as parent_case_status_date
	, cs.parent_case_owner
	, '' as parent_case_eng_site
	, case when cs.soeng_assigned_owner = cs.parent_case_owner then 'true' else 'false' end as esc_retained
	, case when cs.parent_case_owner = esc2.requester_login then 'true' else 'false' end as esc_by_pco
	, cs.escalation_ticket_id
	, esc2.assigned_to_group as ticket_group
	, esc2.assigned_to_individual as ticket_assignee
	, esc2.status as ticket_status
	, esc2.create_date as ticket_create_date
	, esc2.modified_date as ticket_modified_date
	, esc2.resolved_date as ticket_resolved_date
	, esc2.requester_login as ticket_requestor
	, case when isnull(esc2.create_date)= 1 then 0
		when esc2.status in ('Closed','Resolved') then cast(datediff('minutes',cast(esc2.create_date as timestamp),cast(esc2.resolved_date as timestamp))/1440.00 as decimal(5,2))
		else cast(datediff('minutes',cast(esc2.create_date as timestamp),cast(getdate() as timestamp))/1440.00 as decimal(5,2)) end as ticket_aging_raw
	from public.vw_ticket_links cs
left join (select case_id as parent_case_id ,parent_case_owner, status, parent_case_creation_date, status_date  from 
	(select row_number() over (partition by case_id order by message_timestamp desc) as rn
	, case_id
	, cast(creation_date as timestamp) as parent_case_creation_date
	, status
	, assigned_agent_login as parent_case_owner
	, case when status = 'Resolved' then cast(last_outbound_date as timestamp) else getdate() end as Status_date
		from esc_ops.case_data 
		where case_id in 
	(select parent_case_id from (select parent_case_id 
	from (select distinct cd.creation_date,
	cd.case_id as soesc_case_id,
	cd.assigned_agent_login as soeng_assigned_owner,
	listagg(ri.related_item_id, ', ') within group (
		order by ri.creation_date
	) as parent_case_id,
	listagg(co.assigned_agent_login, ', ') within group (
		order by ri.creation_date
	) as parent_case_owner,
	listagg(ri.creation_date, ', ') within group (
		order by ri.creation_date
	) as parent_case_creation_dates
from (
		select *
		from (
				select row_number() over (
						partition by case_id
						order by message_timestamp desc
					) as rn,
					case_id,
					creation_date,
					status,
					assigned_agent_login,
					initial_queue,
					queue,
					operation,
					message_timestamp
				from esc_ops.case_data cd
				where initial_queue = 'EscOps'
					and cd.operation = 'AddCaseAgent'
				order by case_id,
					message_timestamp
			)
		where rn = 1
	) cd
	left join (
		select distinct case_id,
			related_item_id,
			related_item_type,
			associating_agent_login_id,
			cast(creation_date as timestamp) as creation_date
		from esc_ops.case_related_items
		where initial_queue = 'EscOps'
			or queue = 'EscOps'
	) ri on ri.case_id = cd.case_id
	left join (
		select case_id,
			assigned_agent_login
		from (
				select row_number() over (
						partition by case_id
						order by message_timestamp desc
					) as rn,
					case_id,
					assigned_agent_login,
					message_timestamp
				from esc_ops.case_data cd
				where cd.operation = 'AddCaseAgent'
				order by case_id,
					message_timestamp
			)
		where rn = 1
	) co on ri.related_item_id = co.case_id
where ri.related_item_type = 'SupportCase'
	and ri.related_item_id not in (
		select case_id
		from (
				select *
				from (
						select row_number() over (
								partition by case_id
								order by message_timestamp desc
							) as rn,
							case_id,
							creation_date,
							status,
							assigned_agent_login,
							initial_queue,
							queue,
							operation,
							message_timestamp
						from esc_ops.case_data cd
						where initial_queue = 'EscOps'
							and cd.operation = 'AddCaseAgent'
						order by case_id,
							message_timestamp
					)
				where rn = 1
			)
	)
	and ri.associating_agent_login_id = 'automateduser'
group by 1,2,3 )))) where rn = 1) sts on sts.parent_case_id = cs.parent_case_id
left join esc_ops.remedy_esc_tickets esc2 on cs.escalation_ticket_id = esc2.esc_issue_alias) st1) cl
left join esc_ops.remedy_esc_tickets ti on cl.escalation_ticket_id = ti.esc_issue_alias) fcl
left join (select cc_case_id, esc_issue_alias,current_ticket_owner
from (select cr.cc_case_id,
		cr.ticket_id as esc_issue_alias,
		au.description,
		au.to_string,
		ti.assigned_to_group as current_ticket_owner
from esc_ops.remedy_esc_audittrail au
inner join esc_ops.crosswalk cr on cr.ticket_guid = au.case_id
inner join esc_ops.remedy_esc_tickets ti on ti.case_id = au.case_id
where description = 'Assigned Group' 
and isnull(au.from_string,'') != '')
group by 1,2,3) ttg on fcl.escalation_ticket_id = ttg.esc_issue_alias and fcl.parent_case_id = ttg.cc_case_id)esc
left join own_add oa1 on esc.soesc_case_id = oa1.case_id and esc.primary_escops_eng = oa1.assigned_agent_login) esc
	left join own_add oa2 on esc.parent_case_id = oa2.case_id
	and esc.parent_case_owner = oa2.assigned_agent_login
order by soesc_case_id,
	oa2.assignee_num ) esc
order by soesc_case_id, pcase_assign_order
with no schema binding;










