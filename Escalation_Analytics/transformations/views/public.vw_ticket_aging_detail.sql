--DROP VIEW public.vw_ticket_aging_detail;

create or replace view vw_ticket_aging_detail as
with ticket_detail as (
		select esc_issue_alias as ticket_id,
			status,
			assigned_to_group as cur_assigned_ticket_grp,
			cast(create_date as timestamp) as ticket_creation_date,
			cast(
				case
					when resolved_date is null then '1900-01-01 00:00:00.000'
					else resolved_date
				end as timestamp
			) as ticket_resolved_date,
			getdate() as ticket_reporting_date
		from esc_ops.remedy_esc_tickets
	),
	ticket_status as (
		select description as audit_action,
			ticket_id,
			cast(
				case
					when lag(created_date) over (
						partition by ticket_id
						order by datetime_end asc
					) is null then '1900-01-01 00:00:00'
					else lag(created_date) over (
						partition by ticket_id
						order by datetime_end asc
					)
				end as timestamp
			) as datetime_start,
			cast(created_date as timestamp) as datetime_end,
			from_string as owning_target,
			to_string as future_target
		from (
				select cr.ticket_id,
					created_date,
					description,
					from_string,
					to_string
				from esc_ops.remedy_esc_audittrail au
					inner join esc_ops.crosswalk cr on cr.ticket_guid = au.case_id
				where description = 'Status'
					and to_string <> 'Closed'
					and case_id in (
						select case_id
						from esc_ops.remedy_esc_audittrail au
							inner join esc_ops.crosswalk cr on cr.ticket_guid = au.case_id
						where description = 'Status'
							and to_string = 'Assigned'
					)
			)
		where from_string = 'Resolved'
			or to_string = 'Resolved'
			and isnull(from_string, '') != ''
	),
	ticket_status_stage as (
		select st.ticket_id,
			row_number() over (
				partition by st.ticket_id
				order by datetime_end asc
			) as status_rn,
			det.cur_assigned_ticket_grp,
			det.status,
			case
				when st.datetime_start = '1900-01-01 00:00:00' then det.ticket_creation_date
				else st.datetime_start
			end as datetime_start,
			st.datetime_end,
			st.owning_target,
			st.future_target
		from ticket_status st
			inner join ticket_detail det on det.ticket_id = st.ticket_id
	),
	res_status_stage as (
		select row_number() over (
				partition by ticket_id
				order by datetime_end
			) as rn,
			ticket_id,
			cur_assigned_ticket_grp,
			status,
			datetime_start,
			datetime_end,
			owning_target,
			future_target,
			0 as current_flag
		from ticket_status_stage
	),
	final_status_stage as (
		select st.rn + 1,
			st.ticket_id,
			st.cur_assigned_ticket_grp,
			st.status,
			st.datetime_end as datetime_start,
			getdate() as datetime_end,
			st.future_target as owning_target,
			'' as future_target,
			1 as current_flag
		from res_status_stage st
			inner join (
				select ticket_id,
					max(rn) as rn
				from res_status_stage
				group by ticket_id
			) mx on mx.ticket_id = st.ticket_id
			and mx.rn = st.rn
	),
	res_status as (
		select ticket_id,
			cur_assigned_ticket_grp,
			status,
			datetime_start,
			datetime_end,
			owning_target,
			future_target
		from (
				select *
				from res_status_stage
				union all
				select *
				from final_status_stage
			)
		where (
				owning_target = 'Resolved'
				or current_flag = 1
			)
		order by ticket_id,
			rn
	),
	ordered_changes as (
		select ticket_id,
			created_date,
			lag(to_string) over (
				partition by ticket_id
				order by created_date
			) as prev_owner,
			to_string as current_owner,
			from_string as from_owner,
			created_date as change_time
		from (
				select distinct cr.ticket_id,
					cast(aut.created_date as timestamp) as created_date,
					aut.description,
					aut.from_string,
					aut.to_string
				from (
						select case_id,
							created_date,
							description,
							from_string,
							to_string
						from esc_ops.remedy_esc_audittrail
					) aut
					inner join esc_ops.crosswalk cr on cr.ticket_guid = aut.case_id
				where description = 'Assigned Group'
					and case_id in (
						select case_id
						from esc_ops.remedy_esc_audittrail au
							inner join esc_ops.crosswalk cr on cr.ticket_guid = au.case_id
						where description = 'Status'
							and to_string = 'Assigned'
					)
			)
		where isnull(to_string, '') != ''
	),
	grouped_changes as (
		select ticket_id,
			current_owner,
			change_time,
			sum(
				case
					when current_owner <> prev_owner
					or prev_owner is null then 1
					else 0
				end
			) over (
				partition by ticket_id
				order by change_time rows unbounded preceding
			) as owner_group
		from ordered_changes
	),
	timeline_blocks as (
		select ticket_id,
			current_owner as owning_team,
			min(change_time) as start_time,
			lead(min(change_time)) over (
				partition by ticket_id
				order by min(change_time)
			) as next_start_time
		from grouped_changes
		group by ticket_id,
			current_owner,
			owner_group
	),
	ticket_group_assn as (
		select rn,
			ticket_id,
			'Assigned Group' as audit_action,
			created_date,
			start_time as datetime_start,
			end_time as datetime_end,
			from_string as prior_owner,
			to_string as owning_target
		from (
				select row_number() over (
						partition by ticket_id
						order by created_date
					) as rn,
					*
				from (
						select g.ticket_id,
							a.created_date,
							g.start_time,
							g.end_time,
							a.from_string,
							a.to_string
						from (
								select ticket_id,
									owning_team,
									start_time,
									coalesce(next_start_time, getdate()) as end_time
								from timeline_blocks
								order by ticket_id,
									start_time
							) g
							join (
								select distinct cr.ticket_id,
									cast(aut.created_date as timestamp) as created_date,
									aut.description,
									aut.from_string,
									aut.to_string
								from (
										select case_id,
											created_date,
											description,
											from_string,
											to_string
										from esc_ops.remedy_esc_audittrail
									) aut
									inner join esc_ops.crosswalk cr on cr.ticket_guid = aut.case_id
								where description = 'Assigned Group'
									and case_id in (
										select case_id
										from esc_ops.remedy_esc_audittrail au
											inner join esc_ops.crosswalk cr on cr.ticket_guid = au.case_id
										where description = 'Status'
											and to_string = 'Assigned'
									)
							) a on g.ticket_id = a.ticket_id
							and g.owning_team = a.to_string
							and g.start_time = a.created_date
						order by g.ticket_id,
							g.start_time
					)
			) a
		order by 4 asc
	),
	ticket_group_assn_stage as (
		select st.ticket_id,
			st.rn as grp_rn,
			det.cur_assigned_ticket_grp,
			det.status,
			det.ticket_creation_date,
			det.ticket_resolved_date,
			datetime_start,
			st.datetime_end,
			st.prior_owner,
			st.owning_target
		from ticket_group_assn st
			inner join ticket_detail det on det.ticket_id = st.ticket_id
	),
	group_agg as (
		select distinct grp_rn,
			ticket_id,
			cur_assigned_ticket_grp,
			status,
			ticket_creation_date,
			ticket_resolved_date,
			prior_owner,
			owning_target,
			datetime_start,
			datetime_end
		from ticket_group_assn_stage
	),
	grp_st_merge as (
		select distinct *
		from (
				select grp.ticket_id,
					grp.cur_assigned_ticket_grp,
					grp.current_ticket_status,
					grp.ticket_creation_date as create_date,
					grp.ticket_resolved_date as resolved_date,
					grp.owning_target as owning_team,
					grp.prior_owner as previous_owning_team,
					grp.datetime_start,
					grp.datetime_end,
					st.owning_target as owning_status,
					st.datetime_start as status_start,
					st.datetime_end as status_end
				from (
						select ticket_id,
							cur_assigned_ticket_grp,
							status as current_ticket_status,
							ticket_creation_date,
							ticket_resolved_date,
							owning_target,
							prior_owner,
							datetime_start,
							datetime_end
						from (
								select *
								from group_agg
							)
					) grp
					left join(
						select ticket_id,
							cur_assigned_ticket_grp,
							status as current_ticket_status,
							owning_target,
							future_target,
							datetime_start,
							datetime_end
						from (
								select *
								from res_status
							)
						where owning_target = 'Resolved' 
					) st on grp.ticket_id = st.ticket_id
					and st.datetime_start between grp.datetime_start and grp.datetime_end
					and st.datetime_end between grp.datetime_start and grp.datetime_end
			)
		order by ticket_id,
			datetime_end
	)
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
from (select *
from grp_st_merge) 
order by ticket_id, datetime_start, datetime_end

with no schema binding;






