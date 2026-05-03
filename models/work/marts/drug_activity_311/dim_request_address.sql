-- Request address dimension for 311 drug activity requests
WITH addresses AS (
    SELECT
        incident_address,
        incident_zip,
        MAX(city) AS city,
        MAX(street_name) AS street_name,
        MAX(bbl) AS block_lot,
        MIN(council_district) AS council_district
    FROM {{ ref('stg_nyc_311_drug_activity') }}
    WHERE incident_address IS NOT NULL
    GROUP BY
        incident_address,
        incident_zip
),
address_dimension AS (
    SELECT
        {{ dbt_utils.generate_surrogate_key(['incident_address', 'incident_zip']) }} AS request_address_key,
        incident_address AS address,
        city,
        street_name,
        block_lot,
        council_district,
        incident_zip AS zip_code
    FROM addresses
)
SELECT * FROM address_dimension