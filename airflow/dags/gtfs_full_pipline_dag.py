from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.exceptions import AirflowSkipException
from datetime import datetime
import papermill as pm
from azure.identity import ClientSecretCredential
from azure.storage.blob import BlobServiceClient
import psycopg2
import os
from datetime import timedelta
from airflow.utils.trigger_rule import TriggerRule
from zoneinfo import ZoneInfo

# New York Time Zone
ny_tz = ZoneInfo("America/New_York")

# ======================================== Config DBs ========================================
# Azure Config
TENANT_ID = ''
CLIENT_ID = ''
CLIENT_SECRET = ''
ACCOUNT_NAME = 'gtfsdls'
CONTAINER_NAME = 'transitbatchlatest'
PREFIX_LATEST = 'rowdata/latest/'
PREFIX_OLD = 'rowdata/old/'

# PostgreSQL Config
PG_HOST = "postgres"
PG_PORT = 5432
PG_USER = "admin"
PG_PASSWORD = "password"
PG_DATABASE = "gtfs_batch"


# ======================================== Paths ========================================
# Notebooks paths inside the Airflow container
NOTEBOOK_EXTRACT = "/opt/airflow/dags/notebooks/gtfs_extract_workspace/gtfs_extract_and_load_minio.ipynb"
OUTPUT_EXTRACT = "/opt/airflow/dags/notebooks/gtfs_extract_workspace/gtfs_extract_and_load_minio_output.ipynb"

NOTEBOOK_LOAD = "/opt/airflow/dags/notebooks/gtfs_load_to_postgres/gtfs_load_to_postgres.ipynb"
OUTPUT_LOAD = "/opt/airflow/dags/notebooks/gtfs_load_to_postgres/gtfs_load_to_postgres_output.ipynb"

# SQL Scripts paths inside the Airflow container
REFRESH_SQL_FILE = "/opt/airflow/dags/sql/gtfs_refresh_foreign_table.sql"
SCD2_SQL_FILE = "/opt/airflow/dags/sql/gtfs_scd2_script.sql"

# ======================================== ADL utils ========================================
def get_blob_service_client():
    credential = ClientSecretCredential(
        tenant_id=TENANT_ID,
        client_id=CLIENT_ID,
        client_secret=CLIENT_SECRET
    )

    account_url = f"https://{ACCOUNT_NAME}.blob.core.windows.net"
    return BlobServiceClient(account_url=account_url, credential=credential)

def get_container_client():
    return get_blob_service_client().get_container_client(CONTAINER_NAME)


def ensure_container_exists(container_client):
    try:
        container_client.create_container()
        print(f"Container '{CONTAINER_NAME}' created.")
    except Exception:
        print(f"Container '{CONTAINER_NAME}' already exists.")


# ======================================== Tasks ========================================
def run_extract_notebook():
    pm.execute_notebook(NOTEBOOK_EXTRACT, OUTPUT_EXTRACT, parameters={})

def check_adls_data():
    container_client = get_container_client()
    blob_list = list(container_client.list_blobs(name_starts_with=PREFIX_LATEST))
    if not blob_list:
        raise AirflowSkipException("No data found in ADLS path, skipping load step.")
    print(f"Found {len(blob_list)} blobs in {PREFIX_LATEST}")

def run_load_notebook():
    pm.execute_notebook(NOTEBOOK_LOAD, OUTPUT_LOAD, parameters={})

def move_adls_data():
    container_client = get_container_client()
    blob_list = list(container_client.list_blobs(name_starts_with=PREFIX_LATEST))
    if not blob_list:
        print("No files to move from latest to old.")
        return

    for blob in blob_list:
        old_key = blob.name.replace(PREFIX_LATEST, PREFIX_OLD, 1)

        # copy
        source_url = f"https://{ACCOUNT_NAME}.blob.core.windows.net/{CONTAINER_NAME}/{blob.name}"
        new_blob = container_client.get_blob_client(old_key)
        new_blob.start_copy_from_url(source_url)

        # delete
        container_client.delete_blob(blob.name)

    print(f"Moved {len(blob_list)} blobs from {PREFIX_LATEST} to {PREFIX_OLD}")

def run_sql_from_file(sql_file_path):
    with open(sql_file_path, "r") as file:
        sql_script = file.read()

    conn = psycopg2.connect(
        host=PG_HOST,
        port=PG_PORT,
        user=PG_USER,
        password=PG_PASSWORD,
        database=PG_DATABASE,
    )
    conn.autocommit = True
    cur = conn.cursor()
    cur.execute(sql_script)
    conn.commit()
    cur.close()
    conn.close()


# ======================================== DAG ========================================
with DAG(
    dag_id="gtfs_full_pipeline_with_adl_dag",
    #start_date=datetime(2025, 8, 11),
    start_date=datetime(2025, 1, 1, 0, 0, tzinfo=ny_tz),
    #schedule_interval=None,
    schedule_interval="55 23 * * *",
    catchup=False,
    tags=["gtfs", "MinIO", "Web_Scraping", "Spark", "Postgres", "SCD2", "Notebook"],
    default_args={
        "retries": 4,
        "retry_delay": timedelta(minutes=40),
    }
) as dag:

    extract_task = PythonOperator(
        task_id="extract_and_load_adl",
        python_callable=run_extract_notebook
    )

    check_data_task = PythonOperator(
        task_id="check_adl_latest_data",
        python_callable=check_adls_data
    )

    load_task = PythonOperator(
        task_id="load_to_stg_postgres_tables",
        python_callable=run_load_notebook
    )

    move_data_task = PythonOperator(
        task_id="move_data_in_adl",
        python_callable=move_adls_data
    )

    refresh_foreign_table_task = PythonOperator(
        task_id="refresh_foreign_stg_postgres_tables",
        python_callable=lambda: run_sql_from_file(REFRESH_SQL_FILE)
    )

    run_scd2_task = PythonOperator(
        task_id="run_scd2_sql_script",
        python_callable=lambda: run_sql_from_file(SCD2_SQL_FILE)
    )

    trigger_next_dag = TriggerDagRunOperator(
        task_id="trigger_transfer_data_to_clickhouse_dag",
        trigger_dag_id="gtfs_transfer_data_to_clickhouse_dag",
        wait_for_completion=False,
        trigger_rule=TriggerRule.ALL_DONE
    )

    extract_task >> check_data_task >> load_task >> move_data_task >> refresh_foreign_table_task >> run_scd2_task >> trigger_next_dag
