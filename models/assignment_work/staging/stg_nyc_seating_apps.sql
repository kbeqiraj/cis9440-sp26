-- Clean and standardize NYC open restaurant applications data
-- One row per application

WITH source AS (
    SELECT * FROM {{ source('raw', 'source_nyc_open_restaurant_apps') }}
),

cleaned AS (
    SELECT
        * EXCEPT (
            objectid,
            globalid,
            borough,
            postcode,
            restaurant_name,
            legal_business_name,
            doing_business_as,
            building_number,
            street,
            business_address,
            seating_interest,
            food_service_permit_number,
            approved_for_sidewalk_seating,
            approved_for_roadway_seating,
            qualify_alcohol,
            sla_serial_number,
            sla_license_type,
            landmark_district_or_building,
            landmarkdistrict_terms,
            healthcompliance_terms,
            community_board,
            council_district,
            census_tract,
            bin,
            bbl,
            nta
        ),

        -- identifiers
        CAST(objectid AS STRING) AS application_id,
        CAST(globalid AS STRING) AS global_id,

        -- text cleanup
        TRIM(CAST(seating_interest AS STRING)) AS seating_interest,
        TRIM(CAST(restaurant_name AS STRING)) AS restaurant_name,
        TRIM(CAST(legal_business_name AS STRING)) AS legal_business_name,
        TRIM(CAST(doing_business_as AS STRING)) AS doing_business_as,
        TRIM(CAST(building_number AS STRING)) AS building_number,
        TRIM(CAST(street AS STRING)) AS street,
        TRIM(CAST(business_address AS STRING)) AS business_address,
        TRIM(CAST(food_service_permit_number AS STRING)) AS food_service_permit_number,
        TRIM(CAST(sla_serial_number AS STRING)) AS sla_serial_number,
        TRIM(CAST(sla_license_type AS STRING)) AS sla_license_type,
        TRIM(CAST(landmark_district_or_building AS STRING)) AS landmark_district_or_building,
        TRIM(CAST(landmarkdistrict_terms AS STRING)) AS landmarkdistrict_terms,
        TRIM(CAST(healthcompliance_terms AS STRING)) AS healthcompliance_terms,
        TRIM(CAST(community_board AS STRING)) AS community_board,
        TRIM(CAST(council_district AS STRING)) AS council_district,
        TRIM(CAST(census_tract AS STRING)) AS census_tract,
        TRIM(CAST(bin AS STRING)) AS bin,
        TRIM(CAST(bbl AS STRING)) AS bbl,
        TRIM(CAST(nta AS STRING)) AS nta,

        -- borough standardization
        CASE
            WHEN UPPER(TRIM(CAST(borough AS STRING))) IN ('MANHATTAN', 'NEW YORK COUNTY') THEN 'Manhattan'
            WHEN UPPER(TRIM(CAST(borough AS STRING))) IN ('BRONX', 'THE BRONX') THEN 'Bronx'
            WHEN UPPER(TRIM(CAST(borough AS STRING))) IN ('BROOKLYN', 'KINGS COUNTY') THEN 'Brooklyn'
            WHEN UPPER(TRIM(CAST(borough AS STRING))) IN ('QUEENS', 'QUEEN', 'QUEENS COUNTY') THEN 'Queens'
            WHEN UPPER(TRIM(CAST(borough AS STRING))) IN ('STATEN ISLAND', 'RICHMOND COUNTY') THEN 'Staten Island'
            ELSE NULL
        END AS borough,

        -- zip cleanup
        CASE
            WHEN UPPER(TRIM(CAST(postcode AS STRING))) IN ('N/A', 'NA', '') THEN NULL
            WHEN REGEXP_CONTAINS(TRIM(CAST(postcode AS STRING)), r'^\d{5}$') THEN TRIM(CAST(postcode AS STRING))
            WHEN REGEXP_CONTAINS(TRIM(CAST(postcode AS STRING)), r'^\d{5}-\d{4}$') THEN TRIM(CAST(postcode AS STRING))
            ELSE NULL
        END AS postcode,

        -- normalize yes/no style fields
        UPPER(TRIM(CAST(approved_for_sidewalk_seating AS STRING))) AS approved_for_sidewalk_seating,
        UPPER(TRIM(CAST(approved_for_roadway_seating AS STRING))) AS approved_for_roadway_seating,
        UPPER(TRIM(CAST(qualify_alcohol AS STRING))) AS qualify_alcohol,

        CURRENT_TIMESTAMP() AS _stg_loaded_at

    FROM source
    WHERE objectid IS NOT NULL
      AND time_of_submission IS NOT NULL

    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY objectid
        ORDER BY time_of_submission DESC
    ) = 1
)

SELECT * FROM cleaned