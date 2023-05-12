import json
import os

from google.cloud import bigquery, storage
from google.oauth2 import service_account


DATA_FOLDER = "data"
BUSINESS_DOMAIN = "greenery"
location = "asia-southeast1"

project_id = "deb-01-385616"
database_name = "deb_bootcamp_02"

# Load data from Local to GCS
bucket_name = "deb-bootcamp-100039"

keyfile_gcs = "deb-01-385616-b51c59d204f3-stgadm.json"
service_account_info_gcs = json.load(open(keyfile_gcs))
credentials_gcs = service_account.Credentials.from_service_account_info(service_account_info_gcs)

storage_client = storage.Client(
    project=project_id,
    credentials=credentials_gcs,
)
bucket = storage_client.bucket(bucket_name)

# Load data from GCS to BigQuery
keyfile_gbq = "deb-01-385616-90d7ec900f74.json"
service_account_info_gbq = json.load(open(keyfile_gbq))
credentials_gbq = service_account.Credentials.from_service_account_info(service_account_info_gbq)

bigquery_client = bigquery.Client(
    project=project_id,
    credentials=credentials_gbq,
    location=location,
)

def upload_data_to_gcs(data):
    file_path = f"{DATA_FOLDER}/{data}.csv"
    destination_blob_name = f"{BUSINESS_DOMAIN}/{data}.csv"
    blob = bucket.blob(destination_blob_name)
    blob.upload_from_filename(file_path)

    print(f"Uploaded {file_path} to {destination_blob_name}")

def load_tables(data):
    job_config = bigquery.LoadJobConfig(
        skip_leading_rows=1,
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        source_format=bigquery.SourceFormat.CSV,
        autodetect=True,
    )

    destination_blob_name = f"{BUSINESS_DOMAIN}/{data}.csv"
    table_id = f"{project_id}.{database_name}.{data}"
    job = bigquery_client.load_table_from_uri(
        f"gs://{bucket_name}/{destination_blob_name}",
        table_id,
        job_config=job_config,
        location=location,
    )
    job.result()

    table = bigquery_client.get_table(table_id)
    print(f"Loaded {table.num_rows} rows and {len(table.schema)} columns to {table_id}")

def load_partition_tables(data, dt, clustering_fields=None):
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
    destination_blob_name = f"{BUSINESS_DOMAIN}/{data}.csv"
    table_id = f"{project_id}.{database_name}.{data}${partition}"
    job = bigquery_client.load_table_from_uri(
        f"gs://{bucket_name}/{destination_blob_name}",
        table_id,
        job_config=job_config,
        location=location,
    )
    job.result()

    table = bigquery_client.get_table(table_id)
    print(f"Loaded {table.num_rows} rows and {len(table.schema)} columns to {table_id}")

tables = [
    ("addresses"),
    ("order_items"),
    ("products"),
    ("promos"),
]
for table_name in tables:
    upload_data_to_gcs(table_name)
    load_tables(table_name)

partition_tables = [
    ("events", "2021-02-10", None),
    ("orders", "2021-02-10", None),
    ("users", "2020-10-23", ["first_name", "last_name"]),
]
for table_name, dt, clustering_fields in partition_tables:
    upload_data_to_gcs(table_name)
    load_partition_tables(table_name, dt, clustering_fields)