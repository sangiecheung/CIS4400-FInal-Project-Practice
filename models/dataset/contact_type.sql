SELECT
    row_number() OVER () AS contact_type_dim_id, *
FROM
    (SELECT DISTINCT open_data_channel_type
    FROM {{ref ('311_complaints')}})
