with
    source as (select * from {{ source("raw", "source_document_event") }}),

    renamed as (

        select
            value_organisation_id as organisation_id,
            key,
            offset,
            `partition`,
            region,
            timestamp,
            {# document_event_document_created_expiry_end,
        document_event_document_created_expiry_start, #}
            document_event_document_info_document_id as document_id,
            document_event_document_info_document_type_id as document_type_id,
            document_event_document_info_document_version_id as document_version_id,
            document_event_document_info_subject_org_id as upload_org_id,
            document_event_document_info_subject_user_id as upload_user_id,
            document_event_requester_org_id as request_org_id,
            document_event_requester_user_id as request_user_id,

            {# document_event_document_renewed_expiry_end,
        document_event_document_renewed_expiry_start, #}
            date(event_datetime_utc) as event_date,
            event_name,
            message_id,
            service_name,
            user_id,
            _batch_date,
            _timestamp,

            document_event_document_info_expiry_end,
            document_event_document_info_expiry_start,
            document_event_document_info_media_ids,
            document_event_operation

        from source

    )

select *
from renamed
