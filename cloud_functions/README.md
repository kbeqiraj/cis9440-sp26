# Ingestion layer: Cloud Functions

HTTP-triggered Cloud Functions, one per source dataset. Each one queries the NYC Open Data
Socrata API, cleans the response, and writes it into the raw BigQuery dataset. Cloud Scheduler
calls each endpoint on a schedule.

| Function | Socrata dataset | Target table | Filter |
|---|---|---|---|
| `load_311_drug_activity` | `erm2-nwe9` (311 Service Requests) | `nyc_311_drug_activity` | `complaint_type = 'Drug Activity'` |
| `load_shooting_incidents` | `5ucz-vwe8` (NYPD Shootings) | `nyc_shooting_incidents` | none |
| `load_shooting_victims` | `pztn-9bne` (NYPD Shooting Victims) | `nyc_shooting_victims` | none |

The three functions are structurally the same. Only the dataset ID, the schema, and the filter change.

## How a run works

`get_current_offset()` runs a `SELECT COUNT(*)` against the target table and uses the row count as
the Socrata `$offset`. If the table doesn't exist yet the query errors, the offset falls back to 0,
and the function does a full historical load.

It then pulls 5,000 records, ordered by a stable field (`created_date`, `occur_date` or
`incident_key`) so that paging stays consistent between runs.

`clean_record()` handles the response. It drops the Socrata internal fields (`:id`,
`_submission_details`, `__computed_region_*`, and the georeferenced point columns), lowercases and
normalizes the remaining field names for BigQuery, turns empty strings into `None` so they land as
NULLs, and casts ZIP codes to strings so the leading zeros survive.

Before inserting, the function creates the dataset and table if they aren't there, using an
explicit `CORE_BQ_SCHEMA` instead of letting BigQuery autodetect. Autodetection would quietly
change a column type if the source changed; this way the load errors out instead.

`update_bq_schema_if_needed()` compares the incoming record keys against the live table schema and
appends anything new as a `STRING`. NYC Open Data adds columns without warning, and without this
the insert fails.

## Configuration

No project or dataset IDs are hard-coded. Set these as environment variables on each function if you decide it to run the project.

| Variable | Required | Default | Purpose |
|---|---|---|---|
| `BQ_PROJECT_ID` | yes | none | Target GCP project |
| `BQ_DATASET_ID` | no | `nyc_raw` | Target raw dataset |
| `SOCRATA_APP_TOKEN` | recommended | unauthenticated | Raises the Socrata rate limit |


## Limitations

Using `COUNT(*)` as the offset only works for append-only loads. It resumes a forward-only backfill
correctly, but it can't see records the source has revised. If NYC edits an existing 311 request,
this pipeline won't pick up the change. Tracking a high-water mark on `created_date` and running a
`MERGE` on the primary key would fix it, but that's a bigger rewrite than this project needed.

Each invocation loads one chunk of 5,000 rows, so the initial backfill takes a lot of scheduled
runs to catch up instead of finishing in one pass. I kept it this way to stay well inside the
Cloud Functions timeout.
