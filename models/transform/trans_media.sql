with
    media as (select * from {{ ref("stg_source__media") }}),

    media_activity as (

        select
            'MEDIA_UPLOADED' as activity_name,
            *,
            count(key) over (
                partition by event_date, organisation_id
            ) as total_upload_event,
            count(key) over (
                partition by event_date, organisation_id, media_type
            ) as upload_type_event,

        from media

    ),

    media_stats as (

        select
            event_date,
            activity_name,
            organisation_id,
            -- UPLOAD STATS
            total_upload_event,
            -- UPLOAD BY TYPE
            max(
                case when media_type = 'DOCUMENT' then upload_type_event else null end
            ) over (partition by event_date, organisation_id) as upload_document_event,
            max(
                case when media_type = 'VIDEO' then upload_type_event else null end
            ) over (partition by event_date, organisation_id) as upload_video_event,
            max(
                case when media_type = 'IMAGE' then upload_type_event else null end
            ) over (partition by event_date, organisation_id) as upload_image_event

        from media_activity

    )

select *
from media_stats

group by all

order by organisation_id, 1 desc, 4 desc
