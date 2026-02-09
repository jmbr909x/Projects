CREATE OR REPLACE PROCEDURE public.sp_datarefresh()
LANGUAGE plpgsql
AS $$
BEGIN
    BEGIN
        CALL public.sp_case_data_refresh();
        RAISE INFO 'case_data refresh complete';

        CALL public.sp_remedy_esc_audittrail();
        RAISE INFO 'remedy_esc_audittrail refresh complete';

        CALL public.sp_remedy_esc_tickets();
        RAISE INFO 'remedy_esc_tickets refresh complete';

        CALL public.sp_crosswalk();
        RAISE INFO 'sp_crosswalk refresh complete';

        CALL public.sp_case_related_items();
        RAISE INFO 'case_related_items refresh complete';

        CALL public.sp_agent_skills();
        RAISE INFO 'agent_skills refresh complete';

        CALL public.sp_annotation_data();
        RAISE INFO 'annotation_data refresh complete';

        CALL public.sp_case_annotation();
        RAISE INFO 'sp_case_annotation refresh complete';

        COMMIT;
    EXCEPTION
        WHEN OTHERS THEN
            ROLLBACK;
            RAISE;
    END;
END;
$$;
