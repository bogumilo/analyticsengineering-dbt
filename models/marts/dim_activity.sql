with
    media_transformed as (select * from {{ ref("trans_media") }}),

    document_transformed as (select * from {{ ref("trans_document") }}),

    activities_combined as (

        select
            media_transformed.event_date,
            media_transformed.organisation_id,

            {{
                dbt_utils.star(
                    from=ref("trans_media"),
                    relation_alias="media_transformed",
                    except=["event_date", "organisation_id", "activity_name"],
                )
            }},

            {{
                dbt_utils.star(
                    from=ref("trans_document"),
                    relation_alias="document_transformed",
                    except=["event_date", "organisation_id", "activity_name"],
                )
            }},

        from media_transformed

        join document_transformed using (event_date, organisation_id)

    )
select *
from activities_combined
group by all
