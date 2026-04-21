-- Fact table for NYPD shooting victim incidents
WITH victims AS (
    SELECT * FROM {{ ref('stg_nyc_shooting_victims') }}
),
incidents AS (
    SELECT * FROM {{ ref('stg_nyc_shooting_incidents') }}
),
dim_date AS (SELECT * FROM {{ ref('dim_date') }}),
dim_time AS (SELECT * FROM {{ ref('dim_time') }}),
dim_region AS (SELECT * FROM {{ ref('dim_region') }}),
dim_victim AS (SELECT * FROM {{ ref('dim_victim_details') }}),
dim_inc_addr AS (SELECT * FROM {{ ref('dim_incident_address') }}),

fact AS (
    SELECT
        {{ dbt_utils.generate_surrogate_key(['v.victim_id']) }} AS victim_incident_key,
        i.incident_id,
        v.victim_id,
        dv.victim_details_key,
        d.date_key AS incident_date_key,
        t.time_key AS incident_time_key,
        rg.region_key,
        dia.incident_address_key,
        i.latitude AS incident_latitude,
        i.longitude AS incident_longitude
    FROM victims v
    LEFT JOIN incidents i ON v.incident_id = i.incident_id
    LEFT JOIN dim_date d ON i.occur_date = d.full_date
    LEFT JOIN dim_time t ON i.occur_time = t.full_time
    LEFT JOIN dim_region rg ON i.borough = rg.borough
        AND i.police_precinct = rg.police_precinct
    LEFT JOIN dim_victim dv ON v.victim_age_group = dv.age_group
        AND v.victim_sex = dv.gender
        AND v.victim_race = dv.race
        AND v.murder_flag = dv.murder_flag
    LEFT JOIN dim_inc_addr dia ON i.location_desc = dia.incident_address_desc
        AND i.loc_classfctn_desc = dia.incident_address_type
        AND i.loc_of_occur_desc = dia.incident_in_out
)
SELECT * FROM fact