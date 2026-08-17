-- Time dimension shared by 311 drug activity requests and shooting incidents
WITH times_from_311 AS (
    SELECT DISTINCT CAST(created_date AS TIME) AS full_time
    FROM {{ ref('stg_nyc_311_drug_activity') }}
    WHERE created_date IS NOT NULL

    UNION DISTINCT

    SELECT DISTINCT CAST(closed_date AS TIME) AS full_time
    FROM {{ ref('stg_nyc_311_drug_activity') }}
    WHERE closed_date IS NOT NULL

    UNION DISTINCT

    SELECT DISTINCT CAST(updated_date AS TIME) AS full_time
    FROM {{ ref('stg_nyc_311_drug_activity') }}
    WHERE updated_date IS NOT NULL
),
times_from_shooting AS (
    SELECT DISTINCT occur_time AS full_time
    FROM {{ ref('stg_nyc_shooting_incidents') }}
    WHERE occur_time IS NOT NULL
),
all_times AS (
    SELECT * FROM times_from_311
    UNION DISTINCT
    SELECT * FROM times_from_shooting
),
time_dimension AS (
    SELECT
        {{ dbt_utils.generate_surrogate_key(['full_time']) }} AS time_key,
        full_time,
        EXTRACT(HOUR FROM full_time) AS hour,
        EXTRACT(MINUTE FROM full_time) AS minute,
        CASE
            WHEN EXTRACT(HOUR FROM full_time) < 12 THEN 'AM'
            ELSE 'PM'
        END AS am_pm,
        CASE
            WHEN EXTRACT(HOUR FROM full_time) BETWEEN 6 AND 11 THEN 'Morning'
            WHEN EXTRACT(HOUR FROM full_time) BETWEEN 12 AND 16 THEN 'Afternoon'
            WHEN EXTRACT(HOUR FROM full_time) BETWEEN 17 AND 20 THEN 'Evening'
            ELSE 'Night'
        END AS time_of_day_bucket,
        EXTRACT(HOUR FROM full_time) BETWEEN 9 AND 17 AS is_business_hours
    FROM all_times
)
SELECT * FROM time_dimension