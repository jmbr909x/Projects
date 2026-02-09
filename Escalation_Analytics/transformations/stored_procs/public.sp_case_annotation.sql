CREATE OR REPLACE PROCEDURE public.sp_case_annotation() LANGUAGE plpgsql AS $$ BEGIN drop table if exists esc_ops.case_annotation cascade;
create table esc_ops.case_annotation diststyle auto compound sortkey ("case_id", "created_by", "created_time") as with soesc as (
    select case_id,
        initial_queue,
        queue
    from ext_src.case_data
    where (
            initial_queue in (
                'support-tier3',
                'EscOps'
            )
            and operation = 'AddCaseAgent'
        )
        or (
            queue in (
                'support-tier3',
                'EscOps'
            )
        )
        and creation_date >= '2025-01-01 00:00:00'
)
select cast(case_id as varchar(50)) as case_id,
    cast(created_by as varchar(50)) as created_by,
    cast(created_time as timestamp) as created_time
from ext_src.annotation_data
where case_id in (
        select case_id
        from soesc
    )
    and created_time >= '2025-01-01 00:00:00';
COMMIT;
END;
$$

