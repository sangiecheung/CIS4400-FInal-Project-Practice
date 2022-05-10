with complaint_locations as (
    SELECT DISTINCT location_type, incident_address, 
    city, zip_code, borough, landmark, community_board, latitude, longitude
    FROM {{ref ('311_complaints')}}
),
sample_locations as (
    SELECT DISTINCT Sample_Site, longitude, latitude, street_address, city, zipcode
    FROM {{ref ('water_quality')}}
),
combined as (
    SELECT Sample_Site, location_type, 
    COALESCE(incident_address, street_address) as street_address,
    COALESCE(complaint_locations.city, sample_locations.city) as city,
    COALESCE(complaint_locations.zip_code, sample_locations.zipcode) as zip_code,
    borough, landmark, community_board, 
    COALESCE(complaint_locations.latitude, sample_locations.latitude) as latitude, 
    COALESCE(complaint_locations.longitude, sample_locations.longitude) as longitude
FROM complaint_locations 
FULL JOIN sample_locations 
on complaint_locations.latitude=sample_locations.latitude 
and complaint_locations.longitude=sample_locations.longitude)

SELECT row_number() OVER () AS location_dim_id, *
from combined