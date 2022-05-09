SELECT
    row_number() OVER () AS complaint_location_dim_id, *
FROM
    (SELECT DISTINCT location_type, incident_address, 
    street_name, cross_street_1, cross_street_2, city, zip_code, borough, 
    landmark, community_board, latitude, longitude
    FROM {{ref ('311_complaints')}})