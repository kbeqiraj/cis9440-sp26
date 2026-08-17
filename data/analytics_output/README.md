# Analytics layer output

Materialized results of the three BigQuery views defined in
[`analyses/analytics_views.sql`](../../analyses/analytics_views.sql). These are what the Looker
Studio dashboard reads, committed here so the warehouse's output is inspectable without BigQuery
access.

| File | Grain | Rows |
|---|---|---|
| `complaints_and_shootings_by_precinct.csv` | One row per NYPD precinct | 77 |
| `complaints_and_shootings_by_borough.csv` | One row per borough | 5 |
| `monthly_complaints_and_shootings.csv` | One row per month, 2020–2026 | 66 |

The precinct view also carries a `precinct_geom` column holding WKT polygon boundaries for the
choropleth. It is **omitted here**, since it accounted for 3.4 MB of a 3.4 MB export and makes the
file unreadable in a browser. 

---

## Limitations

The exports are the **raw view output**, deliberately unfiltered. The dashboard applies exclusions
downstream. Anyone comparing these files against the headline figures in the main README needs to
know why they differ.

### 1. Precincts 107 and 110 hold two-thirds of all complaints

| Precinct | Borough | Population | Complaints | Per 10k residents |
|---|---|---|---|---|
| **107** | Queens | 151,107 | **47,392** | **3,136.32** |
| **110** | Queens | 172,634 | **8,640** | **500.48** |
| 25 *(highest legitimate)* | Manhattan | 47,405 | 1,317 | 277.82 |

Those two precincts account for **56,032 of 84,627 complaints**, 66.2% of the entire dataset.
Precinct 107’s per-capita rate is 11 times that of the highest genuine precinct.

That is a geocoding artifact and not a real concentration of drug activity. The 311 system assigns
precinct from caller input rather than geocoding the incident. These two appear to act as
fallback values when a request cannot be located. Both are excluded from the precinct-level map.

The same two precincts inflate Queens at the borough level. The borough file shows 
**Queens at 276.87 complaints per 10k against Manhattan’s 54.80**. This fivefold gap is entirely an 
artifact of the two precincts above. Borough comparisons of complaint volume are not reliable without excluding
them; shooting figures are unaffected.

### 2. Complaint volume rises ~24× over the period

Monthly complaints **range from 250 to 500 in 2020 and reach 6,038 by July 2025.** A twenty-fourfold
increase in genuine drug-activity reports over five years is unlikely; this most likely
reflects changes in 311 intake, categorization, or routing rather than actual conditions.
Interpret the trend as a reporting artifact, not a crime trend. Shooting incident counts over the
same period stay broadly flat (35–243 per month), which is the more credible series.

### 3. There is a real gap in the 311 feed

March 2021 records only 14 complaints. **Data from April 2021 through January 2022 is missing entirely.**
The gap exists in the upstream NYC Open Data source and aligns with reduced 311 activity
during COVID. The data has not been interpolated or patched.

Shooting figures are blank for 2026 months because the feed currently ends in December 2025.

### Also filtered upstream

Precinct 22 (Central Park) does not appear in these exports. Its recorded residential population
is 25, making any per-capita rate meaningless. The view applies a population threshold
that excludes it.
