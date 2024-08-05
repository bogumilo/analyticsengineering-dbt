with
    source as (select * from {{ source("raw", "source_typefield_event") }}),

    renamed as (

        select
            value_organisation_id as organisation_id,
            key,
            offset,
            `partition`,
            region,
            timestamp,

            type_modified_at,
            type_operation,
            type_org_id,
            {# type_type_description, #}
            type_type_domain,
            type_type_id,
            type_type_name,
            {# type_type_type_category #}
            date(event_datetime_utc) as event_date,
            {# event_name, #}
            message_id,
            {# service_name, #}
            user_id,
            _batch_date,
            _timestamp,

            type_fields_field_id,
            {# type_fields_required, #}
            index

        from source

    )

select *
from renamed
