-- Incident address dimension for shooting incidents
WITH incident_addresses AS (
    SELECT DISTINCT
        loc_classfctn_desc,
        location_desc,
        loc_of_occur_desc
    FROM {{ ref('stg_nyc_shooting_incidents') }}
    WHERE location_desc IS NOT NULL
),
address_dimension AS (
    SELECT
        {{ dbt_utils.generate_surrogate_key(['loc_classfctn_desc', 'location_desc', 'loc_of_occur_desc']) }} AS incident_address_key,
        loc_classfctn_desc AS incident_address_type,
        location_desc AS incident_address_desc,
        loc_of_occur_desc AS incident_in_out
    FROM incident_addresses
)
SELECT * FROM address_dimension