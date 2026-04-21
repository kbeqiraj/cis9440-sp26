-- Region dimension shared by 311 drug activity requests and shooting incidents
WITH regions_from_311 AS (
    SELECT DISTINCT
        borough,
        CAST(REGEXP_EXTRACT(police_precinct, r'\d+') AS INTEGER) AS police_precinct
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
region_dimension AS (
    SELECT
        {{ dbt_utils.generate_surrogate_key(['borough', 'police_precinct']) }} AS region_key,
        borough,
        police_precinct
    FROM all_regions
)
SELECT * FROM region_dimension