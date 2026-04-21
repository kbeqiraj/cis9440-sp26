-- Fact table for 311 drug activity service requests
WITH requests AS (
    SELECT * FROM {{ ref('stg_nyc_311_drug_activity') }}
),
dim_date AS (SELECT * FROM {{ ref('dim_date') }}),
dim_time AS (SELECT * FROM {{ ref('dim_time') }}),
dim_region AS (SELECT * FROM {{ ref('dim_region') }}),
dim_details AS (SELECT * FROM {{ ref('dim_request_details') }}),
dim_resolution AS (SELECT * FROM {{ ref('dim_request_resolution') }}),
dim_address AS (SELECT * FROM {{ ref('dim_request_address') }}),

fact AS (
    SELECT
        {{ dbt_utils.generate_surrogate_key(['r.request_id']) }} AS request_key,
        r.request_id,
        dd.details_key AS request_details_key,
        dr.request_status_key,
        d_created.date_key AS created_date_key,
        t_created.time_key AS created_time_key,
        d_closed.date_key AS closed_date_key,
        t_closed.time_key AS closed_time_key,
        d_updated.date_key AS updated_date_key,
        t_updated.time_key AS updated_time_key,
        rg.region_key,
        ra.request_address_key,
        r.latitude AS request_latitude,
        r.longitude AS request_longitude
    FROM requests r
    LEFT JOIN dim_date d_created ON DATE(r.created_date) = d_created.full_date
    LEFT JOIN dim_time t_created ON TIME(r.created_date) = t_created.full_time
    LEFT JOIN dim_date d_closed ON DATE(r.closed_date) = d_closed.full_date
    LEFT JOIN dim_time t_closed ON TIME(r.closed_date) = t_closed.full_time
    LEFT JOIN dim_date d_updated ON DATE(r.updated_date) = d_updated.full_date
    LEFT JOIN dim_time t_updated ON TIME(r.updated_date) = t_updated.full_time
    LEFT JOIN dim_region rg ON r.borough = rg.borough
        AND REGEXP_EXTRACT(r.police_precinct, r'\d+') = CAST(rg.police_precinct AS STRING)
    LEFT JOIN dim_details dd ON r.descriptor = dd.request_descriptor
        AND r.method_of_submission = dd.submission_channel
    LEFT JOIN dim_resolution dr ON r.status = dr.resolution_status
        AND r.resolution_description = dr.resolution_details
    LEFT JOIN dim_address ra ON r.incident_address = ra.address
        AND r.incident_zip = ra.zip_code
        AND r.city = ra.city
)
SELECT * FROM fact