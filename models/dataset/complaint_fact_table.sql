with all_complaints as (
    select * from {{ ref('311_complaints') }}
),
agency as (
    select * from {{ ref('agency') }}
),
complaint_type as (
    select * from {{ ref('complaint_type') }}
),
contact_type as (
    select * from {{ ref('contact_type') }}
),
location as (
    select * from {{ ref('location') }}
),
created_dates as (
    select * from {{ ref('dates') }}
),
closed_dates as (
    select * from {{ ref('dates') }}
),
joined as (
    select agency.agency_dim_id, complaint_type.complaint_type_dim_id, 
    contact_type.contact_type_dim_id, location.location_dim_id, 
    created_dates.date_dim_id as created_date_dim_id, closed_dates.date_dim_id as closed_date_dim_id 
    from all_complaints
    left join agency on all_complaints.agency = agency.agency
    left join complaint_type on all_complaints.complaint_type = complaint_type.complaint_type AND all_complaints.descriptor = complaint_type.descriptor
    left join contact_type on all_complaints.open_data_channel_type = contact_type.open_data_channel_type
    left join location on all_complaints.latitude = location.latitude AND all_complaints.longitude = location.longitude
    left join created_dates on all_complaints.created_date = created_dates.full_date
    left join closed_dates on all_complaints.closed_date = closed_dates.full_date    
)
select *, count(*) as Number_of_Complaints
from joined
group by agency_dim_id, complaint_type_dim_id, contact_type_dim_id, location_dim_id, created_date_dim_id, closed_date_dim_id
