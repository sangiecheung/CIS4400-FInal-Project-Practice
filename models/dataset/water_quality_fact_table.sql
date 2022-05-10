with water_quality as (
    select * from {{ ref('water_quality') }}
),
dates as (
    select * from {{ ref('dates') }}
),
location as (
    select * from {{ ref('location') }}
),
sample_information as (
    select * from {{ ref('sample_information') }}
),
joined as (
    select sample_information_dim_id, date_dim_id, location_dim_id, Residual_Free_Chlorine, Turbidity, Fluoride, Coliform, E_coli
    from water_quality
    left join dates on Sample_Date = full_date
    left join location on water_quality.sample_site = location.sample_site AND water_quality.latitude = location.latitude AND water_quality.longitude = location.longitude 
    left join sample_information on water_quality.sample_number = sample_information.sample_number AND water_quality.sample_class = sample_information.sample_class
)
select *, count(*) as Number_of_Complaints
from joined
group by sample_information_dim_id, date_dim_id, location_dim_id, Residual_Free_Chlorine, Turbidity, Fluoride, Coliform, E_coli