with
    mart as (select * from {{ ref("dim_subscription") }}),

    renamed as (

        select
            subscription_date_sk,
            subscription_hk,
            chargify_id_date_sk,

            -- ORG FILTER
            organisation_id,
            organisation_date_sk,
            subscription_id,
            organisation_name,
            chargify_customer_reference_id,
            record_source,
            source_system_code_subtype_description,
            latest_date_flag,
            subscription_status_description,

            -- DATE FILTER
            report_date,
            create_date,
            current_period_end_datetime,
            current_period_start_datetime,
            chargify_id,
            chargify_site_code,
            iauditor_coupon_codes,
            payment_method,
            payment_collection_method,
            chargify_product_type_description,
            currency_code,
            subscription_tier_description,
            billing_frequency_description,
            contract_length_description,
            payment_frequency_description,
            delete_date,
            chargify_trial_start_date,
            chargify_trial_end_date,
            chargify_trial_start_datetime,
            chargify_trial_end_datetime,
            paid_subscription_start_date,
            trial_date,
            earliest_monthly_contract_paid_date,
            earliest_annual_contract_paid_date,
            active_subscription_flag,
            chargify_subscription_status_description,
            renewal_date,
            next_assessment_at,
            activated_at,
            chargify_payment_collection_method,
            customer_id,
            tax_exempt_flag,
            {# chargify_cancellation_message,
            organisation_id_cancellation_message, #}
            -- PRODUCT FILTER
            product_name,
            coupon_codes,
            masked_card_number,
            component_name_list,
            pricing_scheme_list,
            plan_type_description,
            split_billing_description

        from mart

    )

select *
from renamed
