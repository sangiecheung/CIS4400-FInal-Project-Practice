SELECT
    row_number() OVER () AS complaint_type_dim_id, *
FROM
    (SELECT DISTINCT complaint_type, descriptor
    FROM {{ref ('311_complaints')}})