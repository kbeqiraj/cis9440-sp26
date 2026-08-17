-- Region dimension shared by 311 drug activity requests and shooting incidents
WITH regions_from_311 AS (
    SELECT DISTINCT
        borough,
        REGEXP_EXTRACT(police_precinct, r'\d+') AS police_precinct
    FROM {{ ref('stg_nyc_311_drug_activity') }}
    WHERE borough IS NOT NULL
),
regions_from_shooting AS (
    SELECT DISTINCT
        borough,
        police_precinct
    FROM {{ ref('stg_nyc_shooting_incidents') }}
    WHERE borough IS NOT NULL
),
all_regions AS (
    SELECT * FROM regions_from_311
    UNION DISTINCT
    SELECT * FROM regions_from_shooting
),
precinct_pop AS (
    SELECT
        CAST(precinct_2020 AS STRING) AS police_precinct,
        total_pop                      AS precinct_total_pop,
        the_geom                       AS precinct_geom,
        Shape_Length                   AS precinct_shape_length,
        Shape_Area                     AS precinct_shape_area
    FROM {{ source('raw', 'nyc_precincts') }}
),
region_dimension AS (
    SELECT
        {{ dbt_utils.generate_surrogate_key(['r.borough', 'r.police_precinct']) }} AS region_key,
        r.borough,
        r.police_precinct,
        p.precinct_total_pop,
        p.precinct_geom,
        p.precinct_shape_length,
        p.precinct_shape_area
    FROM all_regions r
    LEFT JOIN precinct_pop p
        ON r.police_precinct = p.police_precinct
)
SELECT * FROM region_dimension