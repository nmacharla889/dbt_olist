with source as (

    select * from {{ source('olist_raw', 'olist_order_reviews_dataset') }}

),

renamed as (

    select
        review_id,
        order_id,
        review_score,
        review_comment_title as review_title,
        review_comment_message as review_message,
        review_creation_date as review_timestamp,
        review_answer_timestamp

    from source

)

select * from renamed