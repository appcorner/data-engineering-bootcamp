import json
import os

from google.cloud import bigquery
from google.oauth2 import service_account


DATA_FOLDER = "data"

# keyfile = os.environ.get("KEYFILE_PATH")
keyfile = "deb-01-385616-90d7ec900f74.json"
service_account_info = json.load(open(keyfile))
credentials = service_account.Credentials.from_service_account_info(service_account_info)

project_id = "deb-01-385616"
database_name = "deb_bootcamp"

client = bigquery.Client(
    project=project_id,
    credentials=credentials,
)

def load_tables(table_name):
    job_config = bigquery.LoadJobConfig(
        skip_leading_rows=1,
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        source_format=bigquery.SourceFormat.CSV,
        autodetect=True,
    )

    file_path = f"{DATA_FOLDER}/{table_name}.csv"
    with open(file_path, "rb") as f:
        table_id = f"{project_id}.{database_name}.{table_name}"
        job = client.load_table_from_file(f, table_id, job_config=job_config)
        job.result()

    table = client.get_table(table_id)
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
