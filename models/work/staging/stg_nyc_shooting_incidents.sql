-- Clean and standardize NYPD shooting incident data
-- One row per incident
WITH source AS (
    SELECT * FROM {{ source('raw', 'nyc_shooting_incidents') }}
),
cleaned AS (
    SELECT
        CAST(incident_key AS STRING) AS incident_id,
        CAST(CAST(occur_date AS TIMESTAMP) AS DATE) AS occur_date,
        CAST(occur_time AS TIME) AS occur_time,
        CASE
            WHEN UPPER(TRIM(boro)) = 'MANHATTAN' THEN 'Manhattan'
            WHEN UPPER(TRIM(boro)) = 'BRONX' THEN 'Bronx'
            WHEN UPPER(TRIM(boro)) = 'BROOKLYN' THEN 'Brooklyn'
            WHEN UPPER(TRIM(boro)) = 'QUEENS' THEN 'Queens'
            WHEN UPPER(TRIM(boro)) = 'STATEN ISLAND' THEN 'Staten Island'
            ELSE 'Unknown'
        END AS borough,
        CAST(precinct AS INTEGER) AS police_precinct,
        CAST(jurisdiction_code AS INTEGER) AS jurisdiction_code,
        CAST(location_desc AS STRING) AS location_desc,
        CAST(loc_classfctn_desc AS STRING) AS loc_classfctn_desc,
        CAST(loc_of_occur_desc AS STRING) AS loc_of_occur_desc,
        CAST(latitude AS FLOAT64) AS latitude,
        CAST(longitude AS FLOAT64) AS longitude,
        CAST(x_coord_cd AS STRING) AS x_coord_cd,
        CAST(y_coord_cd AS STRING) AS y_coord_cd,
        CURRENT_TIMESTAMP() AS _stg_loaded_at
    FROM source
    WHERE incident_key IS NOT NULL
      AND occur_date IS NOT NULL
    QUALIFY ROW_NUMBER() OVER (PARTITION BY incident_key ORDER BY occur_date DESC) = 1
)
SELECT * FROM cleaned