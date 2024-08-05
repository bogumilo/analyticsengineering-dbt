with
    docs as (select * from {{ ref("stg_source__document") }}),

    types as (

        select
            {{
                dbt_utils.generate_surrogate_key(
                    [
                        "organisation_id",
                        "key",
                        "offset",
                        "region",
                        "timestamp",
                        "type_modified_at",
                        "type_operation",
                        "type_org_id",
                        "type_type_domain",
                        "type_type_id",
                        "type_type_name",
                        "event_date",
                        "message_id",
                        "user_id",
                        "_batch_date",
                        "_timestamp",
                        "type_fields_field_id",
                        "index",
                    ]
                )
            }} as typefield_hk, *
        from {{ ref("stg_source__document_details") }}

    ),

    document_activity as (

        select
            'API_DOCUMENT' as activity_name,
            *,
            count(key) over (
                partition by event_date, organisation_id
            ) as total_document_event,
            count(key) over (
                partition by event_date, organisation_id, document_event_operation
            ) as document_type_event,

        from docs

    ),

    types_activity as (

        select
            *,
            count(typefield_hk) over (
                partition by event_date, organisation_id
            ) as total_typefield_event,

        from types

    ),

    document_stats as (

        select
            d.event_date,
            d.organisation_id,
            d.activity_name,
            -- DOCUMENT STATS
            d.total_document_event,
            -- DOCUMENT BY TYPE
            max(
                case
                    when d.document_event_operation = 'MEDIA_DELETED_FROM_DOCUMENT'
                    then d.document_type_event
                    else 0
                end
            ) over (partition by d.event_date, d.organisation_id)
            as document_media_deleted_event,
            max(
                case
                    when d.document_event_operation = 'DOCUMENT_CREATED'
                    then d.document_type_event
                    else 0
                end
            ) over (partition by d.event_date, d.organisation_id)
            as document_created_event,
            max(
                case
                    when d.document_event_operation = 'DOCUMENT_DELETED'
                    then d.document_type_event
                    else 0
                end
            ) over (partition by d.event_date, d.organisation_id)
            as document_deleted_event,
            max(
                case
                    when d.document_event_operation = 'DOCUMENT_RENEWED'
                    then d.document_type_event
                    else 0
                end
            ) over (partition by d.event_date, d.organisation_id)
            as document_renewed_event,
            max(
                case
                    when d.document_event_operation = 'DOCUMENT_VERSION_ADDED'
                    then d.document_type_event
                    else 0
                end
            ) over (partition by d.event_date, d.organisation_id)
            as document_version_added_event,
            -- FIELDS STATS
            max(t.total_typefield_event) over (
                partition by d.event_date, d.organisation_id
            ) as total_typefield_event,

        from document_activity d

        left join types_activity t on d.document_type_id = t.type_type_id

    )

select *
from document_stats

group by all

order by organisation_id, 1 desc, 4 desc
