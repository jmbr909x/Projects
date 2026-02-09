CREATE OR REPLACE PROCEDURE public.sp_case_data_refresh() LANGUAGE plpgsql AS $$ BEGIN drop table if exists esc_ops.stage_case_data cascade;
create table esc_ops.stage_case_data diststyle auto compound sortkey ("case_id", "assigned_agent_login", "creation_date") as select case_id,
status,
assigned_agent_login,
initial_queue,
queue,
creation_date,
first_outbound_date,
last_inbound_date,
last_outbound_date,
last_updated_date,
next_response_expiration_date,
response_sla_start_date,
response_sla_minutes,
sla_expiration_date,
start_date,
status_sla_expiration_date,
operation,
message_timestamp
from ext_src.case_data;
COMMIT;
drop table if exists esc_ops.case_data cascade;
create table esc_ops.case_data diststyle auto compound sortkey ("case_id", "assigned_agent_login", "creation_date") as with src as (
    select row_number() over (
            partition by case_id,
            operation
            order by last_updated_date
        ) as rn,
        case_id,
        status,
        assigned_agent_login,
        initial_queue,
        queue,
        creation_date,
        first_outbound_date,
        last_inbound_date,
        last_outbound_date,
        last_updated_date,
        next_response_expiration_date,
        response_sla_start_date,
        response_sla_minutes,
        sla_expiration_date,
        start_date,
        status_sla_expiration_date,
        operation,
        message_timestamp from esc_ops.stage_case_data
),
dedup as (
    select case_id,
        operation,
        max(rn) as rn
    from src
    group by case_id,
        operation
)
select cast(s.case_id as varchar(25)) as case_id,
    cast(s.status as varchar(25)) as status,
    cast(s.assigned_agent_login as varchar(25)) as assigned_agent_login,
    cast(s.initial_queue as varchar(100)) as initial_queue,
    cast(s.queue as varchar(100)) as queue,
    cast(s.creation_date as timestamp) as creation_date,
    cast(s.first_outbound_date as timestamp) as first_outbound_date,
    cast(s.last_inbound_date as timestamp) as last_inbound_date,
    cast(s.last_outbound_date as timestamp) as last_outbound_date,
    cast(s.last_updated_date as timestamp) as last_updated_date,
    cast(s.next_response_expiration_date as timestamp) as next_response_expiration_date,
    cast(s.response_sla_start_date as timestamp) as response_sla_start_date,
    cast(s.response_sla_minutes as int) as response_sla_minutes,
    cast(s.sla_expiration_date as timestamp) as sla_expiration_date,
    cast(s.start_date as timestamp) as start_date,
    cast(s.status_sla_expiration_date as timestamp) as status_sla_expiration_date,
    cast(s.operation as varchar(25)) as operation,
    cast(s.message_timestamp as timestamp) as message_timestamp
    from src s inner
    join dedup d on s.case_id = d.case_id
    and s.operation = d.operation
    and s.rn = d.rn;
COMMIT;
drop table if exists esc_ops.stage_case_data cascade;
COMMIT;
END;
$$
