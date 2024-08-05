with
    facts as (select * from {{ ref("fact_organisation_") }}),

    d_org as (select * from {{ ref("dim_organisation_") }}),

    d_sub as (select * from {{ ref("dim_subscription_") }}),

    d_act as (select * from {{ ref("dim_activity") }})

select
    d_org.organisation_name,
    {{
        dbt_utils.star(
            from=ref("fact_organisation_"),
            relation_alias="facts",
            except=[
                "product_key",
                "customer_key",
                "creditcard_key",
                "ship_address_key",
                "order_status_key",
                "order_date_key",
            ],
        )
    }},
    d_sub.current_period_end_datetime,
    d_sub.current_period_start_datetime,
    {{
        dbt_utils.star(
            from=ref("dim_activity"),
            relation_alias="d_act",
            except=["organisation_id", "event_date"],
        )
    }}

from facts

left join
    d_org
    on (
        facts.organisation_id = d_org.organisation_id
        and facts.report_date = d_org.report_date
    )
left join
    d_sub
    on (
        facts.organisation_id = d_sub.organisation_id
        and facts.report_date = d_sub.report_date
    )
left join
    d_act
    on (
        facts.organisation_id = d_act.organisation_id
        and facts.report_date = d_act.event_date
    )

order by total_upload_event desc, total_document_event desc, report_date desc
