/*
CIS 9440 UTA, SP 2026
Group 12
Milestone 5 — Analysis & Business Intelligence
Project: Exploring Community Drug Activity Reports, Police Responses, and Shooting Incidents in NYC
_______________________________________________________
All queries are written as BigQuery views stored in: kbeqiraj-cis9400.gr_proj_analytics
Source data is pulled from the dimensional model in: kbeqiraj-cis9400.gr_proj_marts
Both datasets have been shared with the Professor on this account: Jaclyn.cohen@baruch.cuny.edu
*/


/*
VIEW 1: complaints_and_shootings_by_precinct

Aggregates drug activity complaints (311) and shooting incidents by NYPD police precinct. Includes precinct population from the 2010 Census and WKT geometry for map rendering in Looker Studio. Per-10k metrics normalize for population differences across precincts. Precincts with population <= 100 are excluded (e.g. Central Park precinct) to avoid extreme per-capita values from near-uninhabited areas.
Addresses KPI 1: Shooting Incident Rate by Borough.
Cross-mart: Joins fact_request (311) and fact_victim_incident (shootings) through dim_region.
*/

CREATE OR REPLACE VIEW `kbeqiraj-cis9400.gr_proj_analytics.complaints_and_shootings_by_precinct` AS

WITH complaints AS (
    SELECT
        r.borough,
        r.police_precinct,
        r.precinct_total_pop,
        ST_GEOGFROMTEXT(r.precinct_geom) AS precinct_geom,
        COUNT(f.request_key) AS total_drug_complaints
    FROM `kbeqiraj-cis9400.gr_proj_marts.fact_request` f
    LEFT JOIN `kbeqiraj-cis9400.gr_proj_marts.dim_region` r
        ON f.region_key = r.region_key
    WHERE r.borough IS NOT NULL
      AND r.borough != 'Unknown'
      AND r.police_precinct IS NOT NULL
      AND r.precinct_total_pop > 100
    GROUP BY r.borough, r.police_precinct, r.precinct_total_pop, r.precinct_geom
),
shootings AS (
    SELECT
        r.police_precinct,
        COUNT(DISTINCT f.incident_id) AS total_shooting_incidents
    FROM `kbeqiraj-cis9400.gr_proj_marts.fact_victim_incident` f
    LEFT JOIN `kbeqiraj-cis9400.gr_proj_marts.dim_region` r
        ON f.region_key = r.region_key
    WHERE r.police_precinct IS NOT NULL
      AND r.precinct_total_pop > 100
    GROUP BY r.police_precinct
)

SELECT
    c.borough,
    c.police_precinct,
    c.precinct_total_pop,
    c.precinct_geom,
    c.total_drug_complaints,
    s.total_shooting_incidents,
    ROUND(c.total_drug_complaints / NULLIF(c.precinct_total_pop, 0) * 10000, 2) AS complaints_per_10k_residents,
    ROUND(s.total_shooting_incidents / NULLIF(c.precinct_total_pop, 0) * 10000, 2) AS shootings_per_10k_residents
FROM complaints c
LEFT JOIN shootings s ON c.police_precinct = s.police_precinct
ORDER BY shootings_per_10k_residents DESC;


/*
VIEW 2: complaints_and_shootings_by_borough

Aggregates precinct-level data up to the borough level by summing across precincts. Population is derived from the precinct view to avoid double-counting (each precinct contributes its population once). Per-10k metrics allow fair comparison across boroughs of different sizes.
Addresses KPI 1: Shooting Incident Rate by Borough.
Cross-mart: built on top of complaints_and_shootings_by_precinct which itself joins both fact tables.
*/

CREATE OR REPLACE VIEW `kbeqiraj-cis9400.gr_proj_analytics.complaints_and_shootings_by_borough` AS

SELECT
    borough,
    SUM(precinct_total_pop) AS borough_population,
    SUM(total_drug_complaints) AS total_drug_complaints,
    SUM(total_shooting_incidents) AS total_shooting_incidents,
    ROUND(SUM(total_drug_complaints) / NULLIF(SUM(precinct_total_pop), 0) * 10000, 2) AS complaints_per_10k_residents,
    ROUND(SUM(total_shooting_incidents) / NULLIF(SUM(precinct_total_pop), 0) * 10000, 2) AS shootings_per_10k_residents
FROM `kbeqiraj-cis9400.gr_proj_analytics.complaints_and_shootings_by_precinct`
GROUP BY borough
ORDER BY shootings_per_10k_residents DESC;

/*
VIEW 3: monthly_complaints_and_shootings

Aggregates drug activity complaints and shooting incidents by calendar month to reveal temporal trends. DATE_TRUNC is used to collapse daily dates to the first of each month, which Looker Studio requires for fendering a proper time series chart. The summer spike pattern (May-August) visible in this view is a key finding: both complaints and shootings peak in warmer months.
Addresses KPI 2: Drug Activity-Shooting Correlation Index (temporal dimension).
Cross-mart: joins fact_request and fact_victim_incident through dim_date.
*/

CREATE OR REPLACE VIEW `kbeqiraj-cis9400.gr_proj_analytics.monthly_complaints_and_shootings` AS

WITH monthly_complaints AS (
    SELECT
        d.year,
        d.month,
        d.month_name,
        DATE_TRUNC(d.full_date, MONTH) AS month_date,
        COUNT(f.request_key) AS total_drug_complaints
    FROM `kbeqiraj-cis9400.gr_proj_marts.fact_request` f
    LEFT JOIN `kbeqiraj-cis9400.gr_proj_marts.dim_date` d
        ON f.created_date_key = d.date_key
    WHERE d.year IS NOT NULL
    GROUP BY d.year, d.month, d.month_name, DATE_TRUNC(d.full_date, MONTH)
),
monthly_shootings AS (
    SELECT
        d.year,
        d.month,
        COUNT(DISTINCT f.incident_id) AS total_shooting_incidents
    FROM `kbeqiraj-cis9400.gr_proj_marts.fact_victim_incident` f
    LEFT JOIN `kbeqiraj-cis9400.gr_proj_marts.dim_date` d ON f.incident_date_key = d.date_key
    WHERE d.year IS NOT NULL
    GROUP BY d.year, d.month
)

SELECT
    c.year,
    c.month,
    c.month_name,
    c.month_date,
    c.total_drug_complaints,
    s.total_shooting_incidents
FROM monthly_complaints c
LEFT JOIN monthly_shootings s ON c.year = s.year AND c.month = s.month
ORDER BY c.year, c.month;


/*
VIEW 4: enforcement_outcomes_by_borough

Categorizes NYPD enforcement responses to drug activity complaints into meaningful outcome buckets (Arrest, Summons, No Action, etc.) using LIKE pattern matching on resolution_details text. Crossed with shooting intensity (shootings_per_10k_residents) from the borough view to examine whether enforcement behavior differs across boroughs with varying levels of shooting activity.
Addresses KPI 4: Enforcement Escalation Rate in High vs Low Violence Areas.
Addresses KPI 5: Drug Activity Enforcement Outcome.
Cross-mart: joins fact_request (311) with shooting data derived from fact_victim_incident via complaints_and_shootings_by_borough.
*/

CREATE OR REPLACE VIEW `kbeqiraj-cis9400.gr_proj_analytics.enforcement_outcomes_by_borough` AS

WITH categorized AS (
    SELECT
        r.borough,
        CASE
            WHEN LOWER(rs.resolution_details) LIKE '%made an arrest%'
              OR LOWER(rs.resolution_details) LIKE '%police made an arrest%'
                THEN 'Arrest'
            WHEN LOWER(rs.resolution_details) LIKE '%issued a summons%'
              OR LOWER(rs.resolution_details) LIKE '%police issued a summons%'
                THEN 'Summons'
            WHEN LOWER(rs.resolution_details) LIKE '%no evidence%'
              OR LOWER(rs.resolution_details) LIKE '%no criminal violation%'
              OR LOWER(rs.resolution_details) LIKE '%action was not necessary%'
              OR LOWER(rs.resolution_details) LIKE '%those responsible%were gone%'
                THEN 'No Action / No Evidence'
            WHEN LOWER(rs.resolution_details) LIKE '%unable to gain entry%'
              OR LOWER(rs.resolution_details) LIKE '%insufficient contact%'
                THEN 'Unable to Investigate'
            WHEN LOWER(rs.resolution_details) LIKE '%does not fall under%'
              OR LOWER(rs.resolution_details) LIKE '%referred to%'
                THEN 'Referred / Out of Jurisdiction'
            ELSE 'Other'
        END AS enforcement_outcome,
        COUNT(f.request_key) AS total_complaints
    FROM `kbeqiraj-cis9400.gr_proj_marts.fact_request` f
    LEFT JOIN `kbeqiraj-cis9400.gr_proj_marts.dim_request_resolution` rs ON f.request_status_key = rs.request_status_key
    LEFT JOIN `kbeqiraj-cis9400.gr_proj_marts.dim_region` r ON f.region_key = r.region_key
    WHERE r.borough IS NOT NULL AND r.borough != 'Unknown'
    GROUP BY r.borough, enforcement_outcome
)

SELECT
    c.borough,
    b.shootings_per_10k_residents,
    b.complaints_per_10k_residents,
    c.enforcement_outcome,
    c.total_complaints,
    ROUND(c.total_complaints * 100.0 / SUM(c.total_complaints) OVER (PARTITION BY c.borough), 2) AS pct_of_borough_complaints
FROM categorized c
LEFT JOIN `kbeqiraj-cis9400.gr_proj_analytics.complaints_and_shootings_by_borough` b ON c.borough = b.borough
ORDER BY b.shootings_per_10k_residents DESC, c.total_complaints DESC;

/*
VIEW 5: correlation_by_borough

Computes the Pearson correlation coefficient between monthly drug complaint counts and monthly shooting incident counts, calculated separately for each borough using BigQuery's built-in CORR() function. Results show no borough has a meaningful positive correlation, with Queens showing the strongest negative relationship (-0.37). This is a key finding: complaint volume does not reliably predict shooting activity at the borough-month level.
Addresses KPI 2: Drug Activity-Shooting Correlation Index.
Cross-mart: joins fact_request and fact_victim_incident through dim_date and dim_region.
*/

CREATE OR REPLACE VIEW `kbeqiraj-cis9400.gr_proj_analytics.correlation_by_borough` AS

WITH monthly_by_borough_complaints AS (
    SELECT
        r.borough,
        d.year,
        d.month,
        COUNT(f.request_key) AS total_drug_complaints
    FROM `kbeqiraj-cis9400.gr_proj_marts.fact_request` f
    LEFT JOIN `kbeqiraj-cis9400.gr_proj_marts.dim_date` d ON f.created_date_key = d.date_key
    LEFT JOIN `kbeqiraj-cis9400.gr_proj_marts.dim_region` r ON f.region_key = r.region_key
    WHERE d.year IS NOT NULL
      AND r.borough IS NOT NULL
      AND r.borough != 'Unknown'
    GROUP BY r.borough, d.year, d.month
),
monthly_by_borough_shootings AS (
    SELECT
        r.borough,
        d.year,
        d.month,
        COUNT(DISTINCT f.incident_id) AS total_shooting_incidents
    FROM `kbeqiraj-cis9400.gr_proj_marts.fact_victim_incident` f
    LEFT JOIN `kbeqiraj-cis9400.gr_proj_marts.dim_date` d
        ON f.incident_date_key = d.date_key
    LEFT JOIN `kbeqiraj-cis9400.gr_proj_marts.dim_region` r
        ON f.region_key = r.region_key
    WHERE d.year IS NOT NULL
      AND r.borough IS NOT NULL
      AND r.borough != 'Unknown'
    GROUP BY r.borough, d.year, d.month
),
combined AS (
    SELECT
        c.borough,
        c.total_drug_complaints,
        s.total_shooting_incidents
    FROM monthly_by_borough_complaints c
    LEFT JOIN monthly_by_borough_shootings s
        ON c.borough = s.borough
        AND c.year = s.year
        AND c.month = s.month
)

SELECT
    borough,
    ROUND(CORR(total_drug_complaints, total_shooting_incidents), 4) AS correlation_coefficient,
    COUNT(*) AS months_of_data,
    CASE
        WHEN CORR(total_drug_complaints, total_shooting_incidents) >= 0.5
            THEN 'Strong Positive'
        WHEN CORR(total_drug_complaints, total_shooting_incidents) >= 0.2
            THEN 'Weak Positive'
        WHEN CORR(total_drug_complaints, total_shooting_incidents) <= -0.5
            THEN 'Strong Negative'
        WHEN CORR(total_drug_complaints, total_shooting_incidents) <= -0.2
            THEN 'Weak Negative'
        ELSE 'No Correlation'
    END AS correlation_strength
FROM combined
GROUP BY borough
ORDER BY correlation_coefficient DESC;


/*
VIEW 6: complaint_shooting_lag_analysis

Tests whether drug activity complaint spikes in one month are associated with changes in shooting incidents in the following 1 or 2 months using the LAG() window function. Key finding: lag correlations are -0.30 (1 month) and -0.39 (2 months), meaning high complaint months actually tend to precede lower shooting months; the opposite of what a simple escalation narrative would predict.
Addresses KPI 3: Violence Escalation Ratio 
Cross-mart: joins fact_request and fact_victim_incident through dim_date.
*/

CREATE OR REPLACE VIEW `kbeqiraj-cis9400.gr_proj_analytics.complaint_shooting_lag_analysis` AS

WITH monthly_complaints AS (
    SELECT
        d.year,
        d.month,
        DATE_TRUNC(d.full_date, MONTH) AS month_date,
        COUNT(f.request_key) AS total_drug_complaints
    FROM `kbeqiraj-cis9400.gr_proj_marts.fact_request` f
    LEFT JOIN `kbeqiraj-cis9400.gr_proj_marts.dim_date` d ON f.created_date_key = d.date_key
    WHERE d.year IS NOT NULL
    GROUP BY d.year, d.month, DATE_TRUNC(d.full_date, MONTH)
),
monthly_shootings AS (
    SELECT
        d.year,
        d.month,
        COUNT(DISTINCT f.incident_id) AS total_shooting_incidents
    FROM `kbeqiraj-cis9400.gr_proj_marts.fact_victim_incident` f
    LEFT JOIN `kbeqiraj-cis9400.gr_proj_marts.dim_date` d
        ON f.incident_date_key = d.date_key
    WHERE d.year IS NOT NULL
    GROUP BY d.year, d.month
),
combined AS (
    SELECT
        c.year,
        c.month,
        c.month_date,
        c.total_drug_complaints,
        s.total_shooting_incidents
    FROM monthly_complaints c
    LEFT JOIN monthly_shootings s
        ON c.year = s.year
        AND c.month = s.month
),
with_lags AS (
    SELECT
        year,
        month,
        month_date,
        total_drug_complaints,
        total_shooting_incidents,
        LAG(total_drug_complaints, 1) OVER (ORDER BY year, month) AS complaints_lag_1month,
        LAG(total_drug_complaints, 2) OVER (ORDER BY year, month) AS complaints_lag_2months
    FROM combined
)

SELECT
    year,
    month,
    month_date,
    total_drug_complaints,
    total_shooting_incidents,
    complaints_lag_1month,
    complaints_lag_2months,
    total_drug_complaints - complaints_lag_1month AS complaint_change_vs_last_month
FROM with_lags
ORDER BY year, month;



-- SUPPLEMENTARY QUERIES USED FOR ANALYSIS 

SELECT * FROM `gr_proj_analytics.correlation_by_borough`;

------
SELECT
    ROUND(CORR(total_drug_complaints, total_shooting_incidents), 4) AS citywide_correlation
FROM `kbeqiraj-cis9400.gr_proj_analytics.monthly_complaints_and_shootings`;

------
SELECT
    ROUND(CORR(complaints_lag_1month, total_shooting_incidents), 4) AS corr_lag_1month,
    ROUND(CORR(complaints_lag_2months, total_shooting_incidents), 4) AS corr_lag_2months
FROM `kbeqiraj-cis9400.gr_proj_analytics.complaint_shooting_lag_analysis`
WHERE complaints_lag_1month IS NOT NULL AND complaints_lag_2months IS NOT NULL;

------
SELECT * FROM kbeqiraj-cis9400.gr_proj_analytics.complaints_and_shootings_by_precinct;
SELECT
    police_precinct,
    precinct_total_pop,
    total_drug_complaints,
    total_shooting_incidents,
    complaints_per_10k_residents,
    shootings_per_10k_residents
FROM `kbeqiraj-cis9400.gr_proj_analytics.complaints_and_shootings_by_precinct`
WHERE police_precinct = '107';

------
SELECT
    police_precinct,
    COUNT(*) AS total_complaints
FROM `kbeqiraj-cis9400.gr_proj_raw_data.nyc_311_drug_activity`
GROUP BY police_precinct
ORDER BY total_complaints DESC
LIMIT 10;

------
select * from kbeqiraj-cis9400.gr_proj_analytics.enforcement_outcomes_by_borough;
------
SELECT
    EXTRACT(YEAR FROM TIMESTAMP(created_date)) AS year,
    EXTRACT(MONTH FROM TIMESTAMP(created_date)) AS month,
    COUNT(*) AS total_complaints
FROM `kbeqiraj-cis9400.gr_proj_raw_data.nyc_311_drug_activity`
GROUP BY year, month
ORDER BY year, month;
------


/*
VIEW 7: shootings_by_time_of_day

Aggregates shooting incidents by time of day bucket to identify which periods of the day see the highest concentration of shootings. Uses dim_time's time_of_day_bucket field which categorizes hours into meaningful periods (e.g. Morning, Afternoon, Evening, Late Night).
Addresses the temporal dimension of shooting incident analysis.
*/
CREATE OR REPLACE VIEW `kbeqiraj-cis9400.gr_proj_analytics.shootings_by_time_of_day` AS

WITH shootings AS (
    SELECT
        t.time_of_day_bucket,
        t.hour,
        COUNT(DISTINCT f.incident_id) AS total_shooting_incidents
    FROM `kbeqiraj-cis9400.gr_proj_marts.fact_victim_incident` f
    LEFT JOIN `kbeqiraj-cis9400.gr_proj_marts.dim_time` t ON f.incident_time_key = t.time_key
    WHERE t.time_of_day_bucket IS NOT NULL
    GROUP BY t.time_of_day_bucket, t.hour
)

SELECT
    time_of_day_bucket,
    hour,
    total_shooting_incidents,
    ROUND(total_shooting_incidents * 100.0 / SUM(total_shooting_incidents) OVER (), 2) AS pct_of_all_shootings
FROM shootings
ORDER BY hour;


/*
VIEW 8: murder_rate_by_borough

Analyzes the share of shooting incidents that resulted in murder by borough, using the murder_flag boolean field from dim_victim_details. Provides context on shooting lethality across boroughs.
Cross-mart: joins fact_victim_incident with dim_region and dim_victim_details.
*/
CREATE OR REPLACE VIEW `kbeqiraj-cis9400.gr_proj_analytics.murder_rate_by_borough` AS

SELECT
    r.borough,
    COUNT(DISTINCT f.incident_id) AS total_shooting_incidents,
    COUNTIF(v.murder_flag = TRUE) AS total_murders,
    ROUND(COUNTIF(v.murder_flag = TRUE) * 100.0
        / NULLIF(COUNT(DISTINCT f.incident_id), 0), 2) AS murder_rate_pct
FROM `kbeqiraj-cis9400.gr_proj_marts.fact_victim_incident` f
LEFT JOIN `kbeqiraj-cis9400.gr_proj_marts.dim_region` r ON f.region_key = r.region_key
LEFT JOIN `kbeqiraj-cis9400.gr_proj_marts.dim_victim_details` v ON f.victim_details_key = v.victim_details_key
WHERE r.borough IS NOT NULL AND r.borough != 'Unknown'
GROUP BY r.borough
ORDER BY murder_rate_pct DESC;


/*
VIEW 9: top_10_hotspot_precincts

Top 10 precincts ranked by a normalized hotspot score. Both complaints_per_10k and shootings_per_10k are min-max scaled to a 0-1 range before averaging, ensuring neither metric dominates the ranking due to scale differences. Includes murder rate per precinct from dim_victim_details.
Cross-mart: joins complaints_and_shootings_by_precinct with murder data from fact_victim_incident and dim_victim_details.
*/
CREATE OR REPLACE VIEW `kbeqiraj-cis9400.gr_proj_analytics.top_10_hotspot_precincts` AS
WITH murder_by_precinct AS (
    SELECT
        r.police_precinct,
        COUNT(DISTINCT f.incident_id) AS total_incidents,
        COUNTIF(v.murder_flag = TRUE) AS total_murders,
        ROUND(COUNTIF(v.murder_flag = TRUE) * 100.0 / NULLIF(COUNT(DISTINCT f.incident_id), 0), 2) AS murder_rate_pct
    FROM `kbeqiraj-cis9400.gr_proj_marts.fact_victim_incident` f
    LEFT JOIN `kbeqiraj-cis9400.gr_proj_marts.dim_region` r ON f.region_key = r.region_key
    LEFT JOIN `kbeqiraj-cis9400.gr_proj_marts.dim_victim_details` v ON f.victim_details_key = v.victim_details_key
    WHERE r.police_precinct IS NOT NULL
    GROUP BY r.police_precinct
),
normalized AS (
    SELECT
        police_precinct,
        borough,
        precinct_total_pop,
        total_drug_complaints,
        total_shooting_incidents,
        complaints_per_10k_residents,
        shootings_per_10k_residents,
        (complaints_per_10k_residents - MIN(complaints_per_10k_residents) OVER ()) /
        NULLIF(MAX(complaints_per_10k_residents) OVER () - MIN(complaints_per_10k_residents) OVER (), 0) AS complaints_normalized,
        (shootings_per_10k_residents - MIN(shootings_per_10k_residents) OVER ()) /
        NULLIF(MAX(shootings_per_10k_residents) OVER () - MIN(shootings_per_10k_residents) OVER (), 0) AS shootings_normalized
    FROM `kbeqiraj-cis9400.gr_proj_analytics.complaints_and_shootings_by_precinct`
    WHERE police_precinct != '107'
)

SELECT
    n.borough,
    n.police_precinct,
    n.precinct_total_pop,
    n.total_drug_complaints,
    n.total_shooting_incidents,
    n.complaints_per_10k_residents,
    n.shootings_per_10k_residents,
    m.murder_rate_pct,
    ROUND((n.complaints_normalized + n.shootings_normalized) / 2, 4) AS hotspot_score
FROM normalized n
LEFT JOIN murder_by_precinct m ON n.police_precinct = m.police_precinct
ORDER BY hotspot_score DESC
LIMIT 10;

-- THE END :((