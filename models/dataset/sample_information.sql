SELECT
    row_number() OVER () AS sample_information_dim_id, *
FROM
    (SELECT DISTINCT sample_number, sample_class
    FROM {{ref ('water_quality')}})
