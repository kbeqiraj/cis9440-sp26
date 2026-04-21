-- Clean and standardize 311 drug activity service request data
-- One row per service request
WITH source AS (
    SELECT * FROM {{ source('raw', 'nyc_311_drug_activity') }}
),
cleaned AS (
    SELECT
        * EXCEPT (
            unique_key,
            created_date,
            closed_date,
            resolution_action_updated_date,
            agency,
            agency_name,
            complaint_type,
            descriptor,
            status,
            incident_zip,
            borough,
            incident_address,
            street_name,
            cross_street_1,
            cross_street_2,
            latitude,
            longitude,
            open_data_channel_type,
            resolution_description
        ),
        -- Identifiers
        CAST(unique_key AS STRING) AS request_id,
        -- Dates
        CAST(created_date AS TIMESTAMP) AS created_date,
        CAST(closed_date AS TIMESTAMP) AS closed_date,
        CAST(resolution_action_updated_date AS TIMESTAMP) AS updated_date,
        -- Request details
        CAST(agency AS STRING) AS agency,
        CAST(agency_name AS STRING) AS agency_name,
        CAST(complaint_type AS STRING) AS complaint_type,
        CAST(descriptor AS STRING) AS descriptor,
        UPPER(TRIM(CAST(status AS STRING))) AS status,
        CAST(resolution_description AS STRING) AS resolution_description,
        -- Zip code cleaning (all zips are 5 digits but keeping for robustness)
        CASE
            WHEN UPPER(TRIM(CAST(incident_zip AS STRING))) IN ('N/A', 'NA') THEN NULL
            WHEN LENGTH(CAST(incident_zip AS STRING)) = 5 THEN CAST(incident_zip AS STRING)
            WHEN LENGTH(CAST(incident_zip AS STRING)) = 9 THEN CAST(incident_zip AS STRING)
            WHEN LENGTH(CAST(incident_zip AS STRING)) = 10
                AND REGEXP_CONTAINS(CAST(incident_zip AS STRING), r'^\d{5}-\d{4}')
                THEN CAST(incident_zip AS STRING)
            ELSE NULL
        END AS incident_zip,
        -- Standardized borough (data is clean, just standardizing casing)
        CASE
            WHEN UPPER(TRIM(borough)) = 'MANHATTAN' THEN 'Manhattan'
            WHEN UPPER(TRIM(borough)) = 'BRONX' THEN 'Bronx'
            WHEN UPPER(TRIM(borough)) = 'BROOKLYN' THEN 'Brooklyn'
            WHEN UPPER(TRIM(borough)) = 'QUEENS' THEN 'Queens'
            WHEN UPPER(TRIM(borough)) = 'STATEN ISLAND' THEN 'Staten Island'
            ELSE 'Unknown'
        END AS borough,
        CAST(incident_address AS STRING) AS incident_address,
        CAST(street_name AS STRING) AS street_name,
        CAST(cross_street_1 AS STRING) AS cross_street_1,
        CAST(cross_street_2 AS STRING) AS cross_street_2,
        CAST(latitude AS FLOAT64) AS latitude,
        CAST(longitude AS FLOAT64) AS longitude,
        CAST(open_data_channel_type AS STRING) AS method_of_submission,
        -- Metadata
        CURRENT_TIMESTAMP() AS _stg_loaded_at
    FROM source
    WHERE unique_key IS NOT NULL
      AND created_date IS NOT NULL
      AND borough IS NOT NULL
    QUALIFY ROW_NUMBER() OVER (PARTITION BY unique_key ORDER BY created_date DESC) = 1
)
SELECT * FROM cleaned