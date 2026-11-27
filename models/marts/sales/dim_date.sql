with date_spine as (
    {{
        dbt_utils.date_spine(
            datepart="day",
            start_date="cast('2016-01-01' as date)",
            end_date="cast('2019-01-01' as date)"
        )
    }}
),

final as (
    select
        cast(date_day as date)                              as date_day,
        cast(date_format(date_day, 'yyyyMMdd') as int)      as date_key,
        year(date_day)                                      as year,
        month(date_day)                                     as month_num,
        date_format(date_day, 'MMMM')                       as month_name,
        date_format(date_day, 'MMM')                        as month_short,
        quarter(date_day)                                   as quarter_num,
        concat('Q', quarter(date_day))                      as quarter_label,
        dayofweek(date_day)                                 as day_of_week,
        date_format(date_day, 'EEEE')                       as day_name,
        date_format(date_day, 'EEE')                        as day_short,
        dayofmonth(date_day)                                as day_of_month,
        dayofyear(date_day)                                 as day_of_year,
        weekofyear(date_day)                                as week_of_year,
        case when dayofweek(date_day) in (1, 7)
            then true else false end                        as is_weekend,
        date_format(date_day, 'yyyy-MM')                    as year_month,
        cast(date_trunc('month', date_day) as date)         as month_start,
        -- Olist order data is only complete from Jan 2017 to Aug 2018. Sep-Dec 2016 and Sep-Oct 2018
        -- hold a handful of orders each, so trend charts filter on this flag to avoid fake spikes and drops.
        case when date_day between cast('{{ var("full_data_start") }}' as date)
                               and cast('{{ var("full_data_end") }}' as date)
            then true else false end                        as is_full_data_month
    from date_spine
)

select * from final