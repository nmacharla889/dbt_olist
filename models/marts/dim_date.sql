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
        date_format(date_day, 'yyyy-MM')                    as year_month
    from date_spine
)

select * from final