import json
import os

from google.cloud import bigquery, storage
from google.oauth2 import service_account


DATA_FOLDER = "data"
BUSINESS_DOMAIN = "greenery"
location = "asia-southeast1"

# keyfile = os.environ.get("KEYFILE_PATH")
keyfile = "deb-01-385616-90d7ec900f74.json"
service_account_info = json.load(open(keyfile))
credentials = service_account.Credentials.from_service_account_info(service_account_info)

keyfile2 = "deb-01-385616-b51c59d204f3-stgadm.json"
service_account_info2 = json.load(open(keyfile2))
credentials2 = service_account.Credentials.from_service_account_info(service_account_info2)

project_id = "deb-01-385616"
database_name = "deb_bootcamp_02"

# Load data from Local to GCS
bucket_name = "deb-bootcamp-100039"

storage_client = storage.Client(
    project=project_id,
    credentials=credentials2,
)
bucket = storage_client.bucket(bucket_name)

# Load data from GCS to BigQuery
bigquery_client = bigquery.Client(
    project=project_id,
    credentials=credentials,
    location=location,
)


table_id = f"{project_id}.{database_name}.{data}"
job_config = bigquery.LoadJobConfig(
    skip_leading_rows=1,
    write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
    source_format=bigquery.SourceFormat.CSV,
    autodetect=True,
)
job = bigquery_client.load_table_from_uri(
    f"gs://{bucket_name}/{destination_blob_name}",
    table_id,
    job_config=job_config,
    location=location,
)
job.result()

table = bigquery_client.get_table(table_id)
print(f"Loaded {table.num_rows} rows and {len(table.schema)} columns to {table_id}")

def upload_data_to_gcs(data):
    file_path = f"{DATA_FOLDER}/{data}.csv"
    destination_blob_name = f"{BUSINESS_DOMAIN}/{data}/{data}.csv"
    blob = bucket.blob(destination_blob_name)
    blob.upload_from_filename(file_path)

def load_tables(data, table_name):
    job_config = bigquery.LoadJobConfig(
        skip_leading_rows=1,
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        source_format=bigquery.SourceFormat.CSV,
        autodetect=True,
    )

    destination_blob_name = f"{BUSINESS_DOMAIN}/{data}/{data}.csv"
    table_id = f"{project_id}.{database_name}.{table_name}"
    job = bigquery_client.load_table_from_uri(
        f"gs://{bucket_name}/{destination_blob_name}",
        table_id,
        job_config=job_config,
        location=location,
    )
    job.result()

    table = bigquery_client.get_table(table_id)
    print(f"Loaded {table.num_rows} rows and {len(table.schema)} columns to {table_id}")

def load_partition_tables(table_name, dt, clustering_fields=None):
    job_config = bigquery.LoadJobConfig(
        skip_leading_rows=1,
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        source_format=bigquery.SourceFormat.CSV,
        autodetect=True,
        time_partitioning=bigquery.TimePartitioning(
            type_=bigquery.TimePartitioningType.DAY,
            field="created_at",
        ),
        clustering_fields=clustering_fields,
    )

    partition = dt.replace("-", "")
    file_path = f"{DATA_FOLDER}/{table_name}.csv"
    with open(file_path, "rb") as f:
        table_id = f"{project_id}.{database_name}.{table_name}${partition}"
        job = client.load_table_from_file(f, table_id, job_config=job_config)
        job.result()

    table = client.get_table(table_id)
    print(f"Loaded {table.num_rows} rows and {len(table.schema)} columns to {table_id}")

tables = [
    ("addresses"),
    ("order_items"),
    ("products"),
    ("promos"),
]
for table_name in tables:
    load_tables(table_name)

partition_tables = [
    ("events", "2021-02-10", None),
    ("orders", "2021-02-10", None),
    ("users", "2020-10-23", ["first_name", "last_name"]),
]
for table_name, dt, clustering_fields in partition_tables:
    load_partition_tables(table_name, dt, clustering_fields)