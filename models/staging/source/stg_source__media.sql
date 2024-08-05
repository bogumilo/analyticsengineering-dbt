with
    source as (select * from {{ source("raw", "source_media_uploaded_event") }}),

    renamed as (

        select
            organisation_id,
            key,
            offset,
            `partition`,
            region,
            timestamp,

            {# media_uploaded_content_length, #}
            media_uploaded_doc_id as doc_id,
            media_uploaded_doc_type as doc_type,
            {# media_uploaded_duration_in_ms, #}
            media_uploaded_media_id as media_id,
            media_uploaded_media_type as media_type,
            media_uploaded_org_id as upload_org_id,
            media_uploaded_user_id as upload_user_id,

            media_uploaded_uploaded_at as uploaded_at,

            date(event_datetime_utc) as event_date,
            event_name,
            message_id,
            service_name,
            user_id,
            _batch_date,
            _timestamp

        from source

    )

select *
from renamed
