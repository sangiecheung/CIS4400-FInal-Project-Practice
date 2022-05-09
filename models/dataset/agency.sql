SELECT
    row_number() OVER () AS agency_dim_id, *
FROM
    (SELECT DISTINCT agency, agency_name
    FROM {{ref ('311_complaints')}})