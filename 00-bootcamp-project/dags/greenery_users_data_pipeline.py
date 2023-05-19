from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils import timezone

import configparser
import csv

import requests
import json
import os

from google.cloud import bigquery, storage
from google.oauth2 import service_account

DAG_FOLDER = "/opt/airflow/dags"

parser = configparser.ConfigParser()
parser.read(f"{DAG_FOLDER}/pipeline.conf")
host = parser.get("api_config", "host")
port = parser.getint("api_config", "port")

API_URL = f"http://{host}:{port}/"

DATA_FOLDER = f"{DAG_FOLDER}/data"

BUSINESS_DOMAIN = "greenery"
LOCATION = "asia-southeast1"

PROJECT_ID = "deb-01-385616"
DATABASE_NAME = "deb_bootcamp_02"

# Load data from Local to GCS
BUCKET_NAME = "deb-bootcamp-100039"
KEYFILE_GCS = f"{DAG_FOLDER}/deb-01-385616-b51c59d204f3-stgadm.json"

# Load data from GCS to BigQuery
KEYFILE_GBQ = f"{DAG_FOLDER}/deb-01-385616-90d7ec900f74.json"

# Import modules regarding GCP service account, BigQuery, and GCS 
# Your code here

def extract_data_api(data, ds, byDate=False):
    if byDate:
        response = requests.get(f"{API_URL}/{data}/?created_at={ds}")
        target_file = f"{DATA_FOLDER}/{data}-{ds}.csv"
    else:
        response = requests.get(f"{API_URL}/{data}")
        target_file = f"{DATA_FOLDER}/{data}-{ds}.csv"

    data = response.json()
    if data:
        with open(target_file, "wt") as f:
            writer = csv.writer(f)
            header = data[0].keys()
            writer.writerow(header)

            for each in data:
                writer.writerow(each.values())
        print(f"Api data is save into file {target_file}")
    else:
        print("No api data")

def upload_data_to_gcs(data, ds):
    # Load data from Local to GCS
    service_account_info_gcs = json.load(open(KEYFILE_GCS))
    credentials_gcs = service_account.Credentials.from_service_account_info(service_account_info_gcs)

    storage_client = storage.Client(
        project=PROJECT_ID,
        credentials=credentials_gcs,
    )
    bucket = storage_client.bucket(BUCKET_NAME)

    destination_blob_name = f"{BUSINESS_DOMAIN}/{data}/{ds}/{data}.csv"
    file_path = f"{DATA_FOLDER}/{data}-{ds}.csv"
    try:
        blob = bucket.blob(destination_blob_name)
        blob.upload_from_filename(file_path)
        print(f"Uploaded {file_path} to {destination_blob_name}")
    except FileNotFoundError:
        print(f"File {file_path} not found")
        pass

def load_tables(data, ds):
    # Load data from GCS to BigQuery
    service_account_info_gbq = json.load(open(KEYFILE_GBQ))
    credentials_gbq = service_account.Credentials.from_service_account_info(service_account_info_gbq)

    bigquery_client = bigquery.Client(
        project=PROJECT_ID,
        credentials=credentials_gbq,
        location=LOCATION,
    )

    job_config = bigquery.LoadJobConfig(
        skip_leading_rows=1,
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        source_format=bigquery.SourceFormat.CSV,
        autodetect=True,
    )

    destination_blob_name = f"{BUSINESS_DOMAIN}/{data}/{ds}/{data}.csv"
    table_id = f"{PROJECT_ID}.{DATABASE_NAME}.{data}"
    job = bigquery_client.load_table_from_uri(
        f"gs://{BUCKET_NAME}/{destination_blob_name}",
        table_id,
        job_config=job_config,
        location=LOCATION,
    )
    job.result()

    table = bigquery_client.get_table(table_id)
    print(f"Loaded {table.num_rows} rows and {len(table.schema)} columns to {table_id}")

def load_partition_tables(data, ds, clustering_fields=None):
    # Validate data from GCS
    service_account_info_gcs = json.load(open(KEYFILE_GCS))
    credentials_gcs = service_account.Credentials.from_service_account_info(service_account_info_gcs)

    storage_client = storage.Client(
        project=PROJECT_ID,
        credentials=credentials_gcs,
    )

    bucket = storage_client.bucket(BUCKET_NAME)

    destination_blob_name = f"{BUSINESS_DOMAIN}/{data}/{ds}/{data}.csv"

    blob = bucket.blob(destination_blob_name)
    if not blob.exists():
        print(f"File {destination_blob_name} not found")
        return
        
    # Load data from GCS to BigQuery
    service_account_info_gbq = json.load(open(KEYFILE_GBQ))
    credentials_gbq = service_account.Credentials.from_service_account_info(service_account_info_gbq)

    bigquery_client = bigquery.Client(
        project=PROJECT_ID,
        credentials=credentials_gbq,
        location=LOCATION,
    )
    
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

    partition = ds.replace("-", "")
    table_id = f"{PROJECT_ID}.{DATABASE_NAME}.{data}${partition}"
    job = bigquery_client.load_table_from_uri(
        f"gs://{BUCKET_NAME}/{destination_blob_name}",
        table_id,
        job_config=job_config,
        location=LOCATION,
    )
    job.result()

    table = bigquery_client.get_table(table_id)
    print(f"Loaded {table.num_rows} rows and {len(table.schema)} columns to {table_id}")

def _extract_data(ds):
    extract_data_api("users", ds, True)

def _load_data_to_gcs(ds):
    upload_data_to_gcs("users", ds)

def _load_data_from_gcs_to_bigquery(ds):
    load_partition_tables("users", ds, ["first_name", "last_name"])


default_args = {
    "owner": "airflow",
		"start_date": timezone.datetime(2020, 1, 1),  # Set an appropriate start date here
}
with DAG(
    dag_id="greenery_users_data_pipeline",  # Replace xxx with the data name
    default_args=default_args,
    schedule="@daily",  # Set your schedule here
    catchup=False,
    tags=["DEB", "2023", "greenery"],
):

    # Extract data from Postgres, API, or SFTP
    extract_data = PythonOperator(
        task_id="extract_data",
        python_callable=_extract_data,
        op_kwargs={"ds": "{{ ds }}"},
    )

    # Load data to GCS
    load_data_to_gcs = PythonOperator(
        task_id="load_data_to_gcs",
        python_callable=_load_data_to_gcs,
        op_kwargs={"ds": "{{ ds }}"},
    )

    # Load data from GCS to BigQuery
    load_data_from_gcs_to_bigquery = PythonOperator(
        task_id="load_data_from_gcs_to_bigquery",
        python_callable=_load_data_from_gcs_to_bigquery,
        op_kwargs={"ds": "{{ ds }}"},
    )

    # Task dependencies
    extract_data >> load_data_to_gcs >> load_data_from_gcs_to_bigquery