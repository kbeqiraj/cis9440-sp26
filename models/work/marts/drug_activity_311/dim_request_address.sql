-- Request address dimension for 311 drug activity requests
WITH addresses AS (
    SELECT DISTINCT
        incident_address,
        city,
        street_name,
        bbl,
        council_district,
        incident_zip
    FROM {{ ref('stg_nyc_311_drug_activity') }}
    WHERE incident_address IS NOT NULL
),
address_dimension AS (
    SELECT
        {{ dbt_utils.generate_surrogate_key(['incident_address', 'incident_zip', 'city']) }} AS request_address_key,
        incident_address AS address,
        city,
        street_name,
        bbl AS block_lot,
        council_district,
        incident_zip AS zip_code
    FROM addresses
)
SELECT * FROM address_dimension