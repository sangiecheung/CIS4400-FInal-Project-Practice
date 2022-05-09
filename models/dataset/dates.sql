with all_dates as 
({{ dbt_utils.date_spine(
    datepart="day",
    start_date="cast('2019-01-01' as date)",
    end_date="cast(date_add(current_date(), INTERVAL 1 DAY) as date)"
   )
}})
 
select row_number() OVER () AS date_dim_id, date_day as full_date, EXTRACT(DAYOFWEEK from date_day) as day_of_week,
EXTRACT(DAY from date_day) as day, EXTRACT(MONTH from date_day) as month, EXTRACT(YEAR from date_day) as year
from all_dates
order by 1
