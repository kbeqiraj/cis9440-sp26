-- Request resolution dimension for 311 drug activity requests
WITH resolutions AS (
    SELECT DISTINCT
        status,
        resolution_description
    FROM {{ ref('stg_nyc_311_drug_activity') }}
    WHERE status IS NOT NULL
),
resolution_dimension AS (
    SELECT
        {{ dbt_utils.generate_surrogate_key(['status', 'resolution_description']) }} AS request_status_key,
        status AS resolution_status,
        resolution_description AS resolution_details
    FROM resolutions
)
SELECT * FROM resolution_dimension