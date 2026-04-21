WITH source AS (
    SELECT * FROM {{ source('raw', 'nyc_shooting_victims') }}
),
cleaned AS (
    SELECT
        CAST(victim_id AS STRING) AS victim_id,
        CAST(incident_key AS STRING) AS incident_id,
        INITCAP(CAST(victim_race AS STRING)) AS victim_race,
        CASE
            WHEN victim_age_group IN ('25-44', '18-24', '<18', '45-64', '65+')
                THEN victim_age_group
            ELSE 'Unknown'
        END AS victim_age_group,
        CASE
            WHEN UPPER(TRIM(victim_sex)) = 'MALE' THEN 'Male'
            WHEN UPPER(TRIM(victim_sex)) = 'FEMALE' THEN 'Female'
            WHEN UPPER(TRIM(victim_sex)) = 'INTERSEX' THEN 'Intersex'
            ELSE 'Unknown'
        END AS victim_sex,
        CASE
            WHEN stat_murder_flg = 'Y' THEN TRUE
            WHEN stat_murder_flg = 'N' THEN FALSE
            ELSE NULL
        END AS murder_flag,
        CURRENT_TIMESTAMP() AS _stg_loaded_at
    FROM source
    WHERE victim_id IS NOT NULL
    QUALIFY ROW_NUMBER() OVER (PARTITION BY victim_id ORDER BY (SELECT NULL)) = 1
)
SELECT * FROM cleaned