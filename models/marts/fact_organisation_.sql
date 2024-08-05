with
    mart as (select * from {{ ref("fact_organisation") }}),

    renamed as (

        select
            organisation_hk,
            organisation_id,
            organisation_date_sk,
            subscription_date_sk,
            report_date,

            -- USERS MONTHLY
            total_monthly_active_user_count,
            paid_monthly_active_user_count,
            full_monthly_active_user_count,
            lite_monthly_active_user_count,
            free_monthly_active_user_count,
            -- PAID USERS MONTHLY BY ACTIVITY
            inspections_paid_monthly_active_user_count,
            actions_paid_monthly_active_user_count,
            issues_paid_monthly_active_user_count,
            heads_up_paid_monthly_active_user_count,
            training_paid_monthly_active_user_count,
            assets_paid_monthly_active_user_count,
            two_product_paid_monthly_active_user_count,
            -- FREE USERS MONTHLY BY ACTIVITY
            inspections_free_monthly_active_user_count,
            actions_free_monthly_active_user_count,
            issues_free_monthly_active_user_count,
            heads_up_free_monthly_active_user_count,
            training_free_monthly_active_user_count,
            assets_free_monthly_active_user_count,
            two_product_free_monthly_active_user_count,
            -- CHARGIFY
            total_chargify_component_type_count,

            -- DAILY USERS
            total_user_count,
            free_user_count,
            paid_user_count,
            full_user_count,
            lite_user_count,

            -- ACTIONS
            total_action_count,
            total_action_completed_count,
            -- ISSUES
            total_issue_count,
            total_issue_resolved_count,
            -- HEADS UPs
            total_heads_up_count,
            total_published_heads_up_count,
            total_heads_up_view_count,
            -- INSPECTION
            commenced_inspection_count,
            commenced_inspection_deleted_count,
            completed_inspection_count,
            completed_inspection_deleted_count,

            -- TEMPLATES
            total_created_templates,
            total_created_templates_deleted,
            -- REPORTS & ANALYTICS
            total_reports_interacted_count,
            total_reports_shared_count,
            total_reports_exported_count,
            total_analytics_event_count,
            total_active_analytics_event_count,
            total_passive_analytics_event_count,
            -- API
            total_api_call_count,
            total_site_count,
            total_site_with_member_count,
            -- SENSORS
            total_activated_hardware_sensor_count,
            total_activated_weather_sensor_count,
            total_deactivated_hardware_sensor_count,
            total_deactivated_weather_sensor_count,

            -- ASSETS
            total_active_asset_count,
            total_archive_asset_count,

            -- SEATS & DEVICES
            total_seat_count,
            change_total_seat_count,
            total_paid_seat_count,
            total_full_seat_count,
            total_free_seat_count,
            total_lite_seat_count,
            total_action_only_seat_count,
            total_issue_or_action_only_seat_count,
            total_issue_only_seat_count,
            total_extra_devices_count,

            -- SUPPORT ETC.
            total_schedule_count,
            total_support_conversation_count,
            total_actionable_support_conversation_count

        from mart

    )

select *
from renamed

order by report_date desc, organisation_id
