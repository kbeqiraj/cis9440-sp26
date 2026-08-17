-- Victim details dimension for shooting incidents
WITH victim_types AS (
    SELECT DISTINCT
        victim_age_group,
        victim_sex,
        victim_race,
        murder_flag
    FROM {{ ref('stg_nyc_shooting_victims') }}
    WHERE victim_age_group IS NOT NULL
),
victim_dimension AS (
    SELECT
        {{ dbt_utils.generate_surrogate_key(['victim_age_group', 'victim_sex', 'victim_race', 'murder_flag']) }} AS victim_details_key,
        victim_age_group AS age_group,
        victim_sex AS gender,
        victim_race AS race,
        murder_flag
    FROM victim_types
)
SELECT * FROM victim_dimension