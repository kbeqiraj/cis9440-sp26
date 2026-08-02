import os
import json
from datetime import datetime
from google.cloud import bigquery
from google.cloud.bigquery import SchemaField
from sodapy import Socrata
import time

# --- Configuration Constants ---
SOCRATA_HOST = 'data.cityofnewyork.us'
SOCRATA_DATASET_ID = 'erm2-nwe9' # The resource ID for NYC 311 Service Requests
# WARNING: Temporarily reduced for manual testing to avoid 'request too large' error due to log volume.
# For production via Cloud Scheduler, you may decide to increase or decrease this value.
CHUNK_SIZE = 5000
ORDER_BY_FIELD = 'created_date' # Field to order by for initial load consistency

# Define the custom Socrata Query Language (SoQL) $where clause here.
# Example 1 (DOT only): SOCRATA_WHERE_CLAUSE = "agency = 'DOT'"
# Example 2 (Complex filter): SOCRATA_WHERE_CLAUSE = "agency = 'DOT' AND complaint_type = 'Street Light Out'"
# Set to None or empty string ('') to ingest all data without filtering.
SOCRATA_WHERE_CLAUSE = "complaint_type = 'Drug Activity'"

# SECRET: Read the Socrata App Token from the Cloud Function environment variables
# Note: This is required for reliable API access.
SOCRATA_APP_TOKEN = os.environ.get('SOCRATA_APP_TOKEN')

# BigQuery Configuration
BQ_PROJECT_ID = os.environ['BQ_PROJECT_ID']
BQ_DATASET_ID = os.environ.get('BQ_DATASET_ID', 'nyc_raw')
BQ_TABLE_ID = 'nyc_311_drug_activity'

# Initialize BigQuery Client
# When BQ_PROJECT_ID is specified, the client is initialized for that project.
bq_client = bigquery.Client(project=BQ_PROJECT_ID)


# Define the CORE BigQuery schema explicitly.
# This list is used for INITIAL table creation and for type enforcement (TIMESTAMP, FLOAT, STRING for incident_zip).
CORE_BQ_SCHEMA = [
    # General Identifiers and Dates (MUST be present for initial table)
    SchemaField("unique_key", "STRING", mode="REQUIRED", description="Unique ID for the service request"),
    SchemaField("created_date", "TIMESTAMP", description="Date and time the request was created"),
    SchemaField("closed_date", "TIMESTAMP", description="Date and time the request was closed"),
    # Agency and Complaint Details
    SchemaField("agency", "STRING", description="Agency code"),
    SchemaField("agency_name", "STRING", description="Full name of the agency"),
    SchemaField("complaint_type", "STRING", description="Type of complaint"),
    SchemaField("descriptor", "STRING", description="Further description of the complaint"),
    # Location Details
    SchemaField("location_type", "STRING", description="Location where the incident occurred"),
    SchemaField("incident_zip", "STRING", description="Zip code of the incident location"), # Explicitly set to STRING
    SchemaField("incident_address", "STRING", description="Street address of the incident"),
    # Geospatial data
    SchemaField("latitude", "FLOAT", description="Latitude coordinate"),
    SchemaField("longitude", "FLOAT", description="Longitude coordinate"),
]

def clean_record(record):
    """
    Cleans up type anomalies (e.g., empty strings to None), removes Socrata
    internal metadata fields, and cleans field names for BigQuery compatibility.
    """
    cleaned_record = {}

    for key, value in record.items():
        # 1. Drop Socrata internal/metadata and computed region fields
        if key.startswith(':') or key in ['_id', '_submission_details', 'location'] or '__computed_region_' in key:
            continue

        # 2. Clean field names for BigQuery (replace problematic characters AND force lowercase)
        # Force lower() before replacing characters to ensure consistency.
        clean_key = key.lower().replace(':', '_').replace('@', '_').replace(' ', '_')

        # 3. Type normalization: Convert empty strings to None for proper NULL handling in BQ
        if value == "":
            cleaned_record[clean_key] = None
        else:
            cleaned_record[clean_key] = value

        # 4. Special handling for zip codes (FORCES STRING CONVERSION)
        # Ensures leading zeros are preserved and BigQuery receives a string.
        if 'zip' in clean_key.lower() and cleaned_record.get(clean_key) is not None:
             cleaned_record[clean_key] = str(cleaned_record[clean_key])

    return cleaned_record

def get_current_offset():
    """
    Reads the current row count from the BigQuery table. This count serves as the
    starting offset for the Socrata API call.
    """
    # Note: We must explicitly use the BQ_PROJECT_ID here since we are querying it.
    query = f"""
        SELECT COUNT(*) AS current_row_count
        FROM `{BQ_PROJECT_ID}.{BQ_DATASET_ID}.{BQ_TABLE_ID}`
    """
    print(f"Executing BQ query to find current row count (offset).")

    try:
        query_job = bq_client.query(query)
        results = query_job.result()

        for row in results:
            # The count will be 0 if the table is empty
            offset = row.current_row_count
            print(f"Retrieved current offset (row count) from BQ: {offset}")
            return offset

    except Exception as e:
        # If the table doesn't exist yet, the query will fail. We treat this as offset 0.
        print(f"Warning: Failed to retrieve count from BQ (likely table/dataset doesn't exist yet): {e}")

    # Default offset to 0 if BQ query fails (triggers full historical load)
    default_offset = 0
    print(f"Defaulting offset to {default_offset}. This will load historical data.")
    return default_offset

def update_bq_schema_if_needed(table_ref, data):
    """
    Checks the incoming data chunk against the current BigQuery table schema
    and adds any new fields found in the data to the table schema.
    """
    try:
        # Get the current table object and schema
        table = bq_client.get_table(table_ref)
        current_schema_fields = {field.name for field in table.schema}

        # Get all unique field names from the current batch of data
        data_fields = set()
        for record in data:
            data_fields.update(record.keys())

        new_fields = data_fields - current_schema_fields

        if new_fields:
            new_schema = list(table.schema)
            print(f"Found {len(new_fields)} new fields in the data: {new_fields}. Adding them to BigQuery schema.")

            for field_name in sorted(list(new_fields)):
                # Default new fields to STRING type.
                new_schema.append(SchemaField(field_name, "STRING", description="Dynamically added by Socrata Ingester"))

            # Update the table schema in BigQuery
            table.schema = new_schema
            bq_client.update_table(table, ["schema"])
            print(f"Successfully updated table schema for {table_ref.table_id}.")
            return True

        return False

    except Exception as e:
        print(f"WARNING: Failed to update BigQuery schema dynamically: {e}")
        return False

def load_data_to_bigquery(data):
    """
    Loads the list of JSON records (data) into the specified BigQuery table
    after ensuring the schema can accommodate the fields in the data.
    """
    # table_ref correctly constructs the full reference using the project/dataset/table IDs
    table_ref = bq_client.dataset(BQ_DATASET_ID).table(BQ_TABLE_ID)
    # Handle potential errors
    max_retries = 3
    delay_seconds = 5

    try:
        # 1. Ensure the Dataset exists
        try:
            # Use the BQ_PROJECT_ID here to ensure dataset creation happens in the defined project
            dataset_ref = bq_client.dataset(BQ_DATASET_ID, project=BQ_PROJECT_ID)
            bq_client.get_dataset(dataset_ref)
        except Exception:
            print(f"Dataset {BQ_DATASET_ID} not found in project {BQ_PROJECT_ID}. Creating it.")
            dataset = bigquery.Dataset(dataset_ref)
            bq_client.create_dataset(dataset)

        # 2. Ensure the Table exists with the CORE schema
        table_exists = False
        try:
            bq_client.get_table(table_ref)
            table_exists = True
        except Exception:
            print(f"Table {BQ_TABLE_ID} not found. Creating table with CORE schema.")
            table = bigquery.Table(table_ref, schema=CORE_BQ_SCHEMA)
            bq_client.create_table(table)
            table_exists = True
            print(f"Created table {BQ_TABLE_ID}.")

        if not table_exists:
            raise RuntimeError("Could not confirm or create BigQuery table.")

        # 2. Dynamic Schema Update (Checks incoming keys against existing schema)
        schema_updated = update_bq_schema_if_needed(table_ref, data)

        # 3. Stream data insertion with retries
        for attempt in range(max_retries):
            # The data passed to insert_rows_json MUST be the cleaned data
            errors = bq_client.insert_rows_json(table_ref, data)

            if not errors:
                print(f"Successfully loaded {len(data)} rows into BigQuery.")
                return True

            # If errors occur AND a schema update just happened, wait and retry.
            if schema_updated and attempt < max_retries - 1:
                print(f"Insertion failed, but schema was recently updated. Retrying in {delay_seconds} seconds (Attempt {attempt + 2}/{max_retries}).")
                time.sleep(delay_seconds)
            else:
                # Permanent insertion error or last retry failed
                print(f"Final attempt failed. Errors inserting rows: {json.dumps(errors, indent=2)}")
                return False


    except Exception as e:
        print(f"Critical BigQuery Loading Error: {e}")
        return False

def ingest_socrata_data(request):
    """
    Main function triggered by HTTP request (manual or Cloud Scheduler).
    The 'request' parameter is required for an HTTP-triggered function.
    """
    print(f"Starting Socrata data ingestion job at {datetime.now().isoformat()}")

    # 1. Determine the current offset by counting rows in the BigQuery table
    current_offset = get_current_offset()

    # Initialize Socrata Client
    try:
        socrata_client = Socrata(
            SOCRATA_HOST,
            SOCRATA_APP_TOKEN,
            timeout=300 # Set a generous timeout for the API call
        )
        if SOCRATA_APP_TOKEN:
            print(f"Socrata Client initialized for host: {SOCRATA_HOST} (Authenticated)")
        else:
            print(f"Socrata Client initialized for host: {SOCRATA_HOST} (Unauthenticated)")

    except Exception as e:
        print(f"Error initializing Socrata client: {e}")
        return "Failed to initialize Socrata client.", 500 # Return HTTP error on failure


    # 2. Construct the Socrata API query using the offset
    query_params = {
        '$limit': CHUNK_SIZE,
        '$offset': current_offset,
        '$order': f'{ORDER_BY_FIELD} ASC', # Ensure a consistent order for continuous loading
    }

    # Apply the manual Socrata filter if configured
    if SOCRATA_WHERE_CLAUSE:
        query_params['$where'] = SOCRATA_WHERE_CLAUSE
        print(f"Applying manual filter: {query_params['$where']}")

    print(f"Fetching data: limit={CHUNK_SIZE}, offset={current_offset}, dataset={SOCRATA_DATASET_ID}")

    try:
        # Fetch the data chunk using the sodapy client
        data = socrata_client.get(
            SOCRATA_DATASET_ID,
            **query_params
        )
        socrata_client.close() # Close the client connection after use

    except Exception as e:
        print(f"API Fetch Error using sodapy: {e}")
        return "Failed to fetch data from Socrata API.", 500 # Return HTTP error on failure

    rows_fetched = len(data)

    # 3. Process the returned data
    if rows_fetched > 0:
        print(f"Successfully fetched {rows_fetched} rows. Cleaning and loading into BigQuery...")

        # Pre-process the data for field name cleanup and removing metadata fields
        cleaned_data = [clean_record(record) for record in data]

        # Load the cleaned data.
        if not load_data_to_bigquery(cleaned_data):
             return "Data loading failed in BigQuery. Check logs for details.", 500
        # Note: The new offset is automatically reflected in the next BQ COUNT(*) query.

    else:
        # 4. Handle the end of the data set
        print("API returned 0 rows. The data set is caught up to the latest available records.")

    print("Ingestion function completed successfully.")

    # Return HTTP success
    return "Ingestion job completed successfully.", 200
