import os
import json
from datetime import datetime
from google.cloud import bigquery
from google.cloud.bigquery import SchemaField
from sodapy import Socrata
import time

# --- Configuration Constants ---
SOCRATA_HOST = 'data.cityofnewyork.us'
SOCRATA_DATASET_ID = 'pztn-9bne'  # NYC Shooting Victims (2006-Present)
CHUNK_SIZE = 5000
ORDER_BY_FIELD = 'incident_key'  # Primary date field for this dataset

# No WHERE clause needed — this dataset is already scoped to shootings only
SOCRATA_WHERE_CLAUSE = None

SOCRATA_APP_TOKEN = os.environ.get('SOCRATA_APP_TOKEN')

# BigQuery Configuration
BQ_PROJECT_ID = os.environ['BQ_PROJECT_ID']
BQ_DATASET_ID = os.environ.get('BQ_DATASET_ID', 'nyc_raw')
BQ_TABLE_ID = 'nyc_shooting_victims'

# Initialize BigQuery Client
bq_client = bigquery.Client(project=BQ_PROJECT_ID)


CORE_BQ_SCHEMA = [
    SchemaField("incident_key",      "INTEGER", mode="REQUIRED", description="Randomly generated persistent ID for each incident"),
    SchemaField("victim_id",         "STRING",  mode="REQUIRED", description="Randomly generated persistent ID for each victim"),
    SchemaField("victim_age_group",  "STRING",  description="Victim's age within a category"),
    SchemaField("victim_sex",        "STRING",  description="Victim's sex description"),
    SchemaField("victim_race",       "STRING",  description="Victim's race description"),
    SchemaField("stat_murder_flg",   "STRING",  description="Shooting resulted in the victim's death which would be counted as a murder"),
]


def clean_record(record):
    """
    Cleans up type anomalies, removes Socrata internal metadata fields,
    and cleans field names for BigQuery compatibility.
    """
    cleaned_record = {}

    for key, value in record.items():
        if key.startswith(':') or key in ['_id', '_submission_details', 'location'] or '__computed_region_' in key:
            continue
        # Drop the georeferenced point column — lat/lon cover this
        if 'georeferenced' in key.lower() or 'new georeferenced' in key.lower():
            continue

        clean_key = key.lower().replace(':', '_').replace('@', '_').replace(' ', '_')

        if value == "":
            cleaned_record[clean_key] = None
        else:
            cleaned_record[clean_key] = value

        if 'zip' in clean_key.lower() and cleaned_record.get(clean_key) is not None:
            cleaned_record[clean_key] = str(cleaned_record[clean_key])

    return cleaned_record


def get_current_offset():
    query = f"""
        SELECT COUNT(*) AS current_row_count
        FROM `{BQ_PROJECT_ID}.{BQ_DATASET_ID}.{BQ_TABLE_ID}`
    """
    print(f"Executing BQ query to find current row count (offset).")

    try:
        query_job = bq_client.query(query)
        results = query_job.result()
        for row in results:
            offset = row.current_row_count
            print(f"Retrieved current offset (row count) from BQ: {offset}")
            return offset
    except Exception as e:
        print(f"Warning: Failed to retrieve count from BQ (likely table doesn't exist yet): {e}")

    print("Defaulting offset to 0. This will load historical data.")
    return 0


def update_bq_schema_if_needed(table_ref, data):
    try:
        table = bq_client.get_table(table_ref)
        current_schema_fields = {field.name for field in table.schema}

        data_fields = set()
        for record in data:
            data_fields.update(record.keys())

        new_fields = data_fields - current_schema_fields

        if new_fields:
            new_schema = list(table.schema)
            print(f"Found {len(new_fields)} new fields: {new_fields}. Adding to BigQuery schema.")
            for field_name in sorted(list(new_fields)):
                new_schema.append(SchemaField(field_name, "STRING", description="Dynamically added by Socrata Ingester"))
            table.schema = new_schema
            bq_client.update_table(table, ["schema"])
            print(f"Successfully updated table schema for {table_ref.table_id}.")
            return True

        return False
    except Exception as e:
        print(f"WARNING: Failed to update BigQuery schema dynamically: {e}")
        return False


def load_data_to_bigquery(data):
    table_ref = bq_client.dataset(BQ_DATASET_ID).table(BQ_TABLE_ID)
    max_retries = 3
    delay_seconds = 5

    try:
        try:
            dataset_ref = bq_client.dataset(BQ_DATASET_ID, project=BQ_PROJECT_ID)
            bq_client.get_dataset(dataset_ref)
        except Exception:
            print(f"Dataset {BQ_DATASET_ID} not found. Creating it.")
            dataset = bigquery.Dataset(dataset_ref)
            bq_client.create_dataset(dataset)

        try:
            bq_client.get_table(table_ref)
        except Exception:
            print(f"Table {BQ_TABLE_ID} not found. Creating with CORE schema.")
            table = bigquery.Table(table_ref, schema=CORE_BQ_SCHEMA)
            bq_client.create_table(table)
            print(f"Created table {BQ_TABLE_ID}.")

        schema_updated = update_bq_schema_if_needed(table_ref, data)

        for attempt in range(max_retries):
            errors = bq_client.insert_rows_json(table_ref, data)
            if not errors:
                print(f"Successfully loaded {len(data)} rows into BigQuery.")
                return True
            if schema_updated and attempt < max_retries - 1:
                print(f"Insertion failed after schema update. Retrying in {delay_seconds}s (Attempt {attempt + 2}/{max_retries}).")
                time.sleep(delay_seconds)
            else:
                print(f"Final attempt failed. Errors: {json.dumps(errors, indent=2)}")
                return False

    except Exception as e:
        print(f"Critical BigQuery Loading Error: {e}")
        return False


def ingest_socrata_data(request):
    print(f"Starting NYPD Shootings ingestion job at {datetime.now().isoformat()}")

    current_offset = get_current_offset()

    try:
        socrata_client = Socrata(SOCRATA_HOST, SOCRATA_APP_TOKEN, timeout=300)
        print(f"Socrata Client initialized ({'Authenticated' if SOCRATA_APP_TOKEN else 'Unauthenticated'})")
    except Exception as e:
        print(f"Error initializing Socrata client: {e}")
        return "Failed to initialize Socrata client.", 500

    query_params = {
        '$limit': CHUNK_SIZE,
        '$offset': current_offset,
        '$order': f'{ORDER_BY_FIELD} ASC',
    }

    if SOCRATA_WHERE_CLAUSE:
        query_params['$where'] = SOCRATA_WHERE_CLAUSE

    print(f"Fetching data: limit={CHUNK_SIZE}, offset={current_offset}, dataset={SOCRATA_DATASET_ID}")

    try:
        data = socrata_client.get(SOCRATA_DATASET_ID, **query_params)
        socrata_client.close()
    except Exception as e:
        print(f"API Fetch Error: {e}")
        return "Failed to fetch data from Socrata API.", 500

    rows_fetched = len(data)

    if rows_fetched > 0:
        print(f"Fetched {rows_fetched} rows. Cleaning and loading into BigQuery...")
        cleaned_data = [clean_record(record) for record in data]
        if not load_data_to_bigquery(cleaned_data):
            return "Data loading failed. Check logs for details.", 500
    else:
        print("API returned 0 rows. Dataset is fully caught up.")

    print("Ingestion function completed successfully.")
    return "Ingestion job completed successfully.", 200
