with complaints as 
    (select  
        cast(created_date as date) as created_date, 
        cast(closed_date as date) as closed_date,
        agency,
        agency_name,
        complaint_type,
        descriptor,
        location_type,
        incident_zip as zip_code,
        incident_address,
        street_name,
        cross_street_1,
        cross_street_2,
        intersection_street_1,
        intersection_street_2,
        city, 
        landmark, 
        community_board, 
        borough,
        latitude, 
        longitude, 
        open_data_channel_type
    from `cis4400-final-project.dataset.311_complaint`
    WHERE latitude IS NOT NULL AND longitude IS NOT NULL
)

select * from complaints
