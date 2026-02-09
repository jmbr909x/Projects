--DROP VIEW public.vw_ticket_links;
s
CREATE OR REPLACE VIEW public.vw_ticket_links
AS 
select distinct l.creation_date,
				l.soesc_case_id,
				l.soeng_assigned_owner,
				cast(l.parent_case_id as varchar(500)) as parent_case_id,
			    cast(l.parent_case_owner as varchar(500)) as parent_case_owner,
				cast(case when cw.ticket_id is null then 'Not Escalated Further' else cw.ticket_id end as varchar(20)) as escalation_ticket_id
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
group by 1,2,3 ) l
left join (select distinct * from esc_ops.crosswalk) cw on cw.cc_case_id = l.parent_case_id
where parent_case_id not like '%,%'
order by l.soesc_case_id, l.creation_date desc 
with no schema binding;






