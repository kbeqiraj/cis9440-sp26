-- Request details dimension for 311 drug activity requests
WITH details AS (
    SELECT DISTINCT
        descriptor,
        method_of_submission
    FROM {{ ref('stg_nyc_311_drug_activity') }}
    WHERE descriptor IS NOT NULL
),
details_dimension AS (
    SELECT
        {{ dbt_utils.generate_surrogate_key(['descriptor', 'method_of_submission']) }} AS details_key,
        descriptor AS request_descriptor,
        method_of_submission AS submission_channel
    FROM details
)
SELECT * FROM details_dimension