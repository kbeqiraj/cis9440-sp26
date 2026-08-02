# Ingestion layer: Cloud Functions

Three HTTP-triggered Google Cloud Functions, one per source dataset. Each queries the NYC Open
Data Socrata API, cleans the response, and streams it into the raw BigQuery dataset. Cloud
Scheduler hits each endpoint on a cadence.

| Function | Socrata dataset | Target table | Filter |
|---|---|---|---|
| `load_311_drug_activity` | `erm2-nwe9` (311 Service Requests) | `nyc_311_drug_activity` | `complaint_type = 'Drug Activity'` |
| `load_shooting_incidents` | `5ucz-vwe8` (NYPD Shootings) | `nyc_shooting_incidents` | none |
| `load_shooting_victims` | `pztn-9bne` (NYPD Shooting Victims) | `nyc_shooting_victims` | none |

All three share the same structure; only the dataset, schema, and filter differ.

---

## How each run works

1. **Determine where to resume.** `get_current_offset()` runs `SELECT COUNT(*)` against the target
   table and uses the row count as the Socrata `$offset`. If the table doesn't exist yet the query
   fails, the offset falls back to 0, and the function performs a full historical load.
2. **Fetch one chunk.** 5,000 records per invocation, ordered by a stable field (`created_date`,
   `occur_date`, or `incident_key`) so paging stays consistent across runs.
3. **Clean each record.** `clean_record()` drops Socrata internal fields (`:id`, `_submission_details`,
   `__computed_region_*`, georeferenced point columns), lowercases and normalizes field names for
   BigQuery, converts empty strings to `None` so they land as proper NULLs, and forces ZIP codes to
   strings so leading zeros survive.
4. **Ensure the target exists.** Creates the dataset and table if missing, using an explicit
   `CORE_BQ_SCHEMA` rather than autodetection — a source-side type change then fails loudly instead
   of silently corrupting a column.
5. **Absorb new source columns.** `update_bq_schema_if_needed()` diffs the incoming record keys
   against the live table schema and appends any new fields as `STRING`. NYC Open Data adds columns
   without notice; this keeps ingestion from breaking when that happens.
6. **Insert with retries.** Up to three attempts, five seconds apart, because a schema update isn't
   immediately visible to the streaming API.

## Configuration

No project or dataset identifiers are committed. Set these as environment variables on each
function:

| Variable | Required | Default | Purpose |
|---|---|---|---|
| `BQ_PROJECT_ID` | yes | none | Target GCP project |
| `BQ_DATASET_ID` | no | `nyc_raw` | Target raw dataset |
| `SOCRATA_APP_TOKEN` | recommended | unauthenticated | Raises the Socrata rate limit |

## Deploying

```bash
cd load_311_drug_activity
gcloud functions deploy load-311nyc-drug-activity-data \
  --runtime python311 \
  --trigger-http \
  --entry-point ingest_socrata_data \
  --set-env-vars BQ_PROJECT_ID=your-project,BQ_DATASET_ID=nyc_raw \
  --set-secrets SOCRATA_APP_TOKEN=socrata-token:latest
```

Repeat for the other two, then point a Cloud Scheduler job at each URL.

---

## Known limitations

Both are deliberate tradeoffs, not oversights.

**`COUNT(*)` as the offset is append-only.** It resumes correctly for a forward-only backfill, but
it can't detect records the source has *revised*. If NYC edits an existing 311 request, this
pipeline won't pick the change up. A production version would track a high-water mark on
`created_date` and `MERGE` on the primary key instead of streaming inserts.

**One chunk per invocation.** Each call loads 5,000 rows, so the initial backfill takes many
scheduled runs to catch up rather than completing in one pass. That keeps each execution well
inside the Cloud Functions timeout, which was the priority during development.
