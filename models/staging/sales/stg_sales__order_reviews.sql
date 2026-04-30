with source as (

    select * from {{ source('sales', 'olist_order_reviews_dataset') }} qualify row_number() over (partition by review_id order by review_answer_timestamp desc) = 1

),

renamed as (

    select
        review_id,
        order_id,
        review_score,
        review_comment_title as review_title,
        review_comment_message as review_message,
        date(convert_timezone('America/Sao_Paulo', 'UTC', review_creation_date)) as review_date,
        date(convert_timezone('America/Sao_Paulo', 'UTC', review_answer_timestamp)) as review_answer_date

    from source

)

select * from renamed