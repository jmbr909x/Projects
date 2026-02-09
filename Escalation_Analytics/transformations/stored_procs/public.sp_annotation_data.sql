CREATE OR REPLACE PROCEDURE public.sp_annotation_data() LANGUAGE plpgsql AS $$ BEGIN drop table if exists esc_ops.annotation_data cascade;
create table esc_ops.annotation_data diststyle auto compound sortkey ("case_id", "created_by", "created_time") as
SELECT cast(case_id as varchar(50)) as case_id,
    cast(communication_type as varchar(50)) as communication_type,
    cast(content as varchar(16383)) as content,
    cast(created_by as varchar(50)) as created_by,
    cast(created_time as timestamp) as created_time,
    cast(content_ext as varchar(16383)) as content_ext,
    cast(tenant_id as varchar(50)) as tenant_id
from ext_src.annotation_data;
COMMIT;
END;
$$
