from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.exceptions import AirflowSkipException
from datetime import datetime
import papermill as pm
from minio import Minio
from minio.commonconfig import CopySource
import psycopg2
import os

# MinIO config
MINIO_ENDPOINT = "minio:9000"
MINIO_ACCESS_KEY = "minio"
MINIO_SECRET_KEY = "minio123"
BUCKET_NAME = "transitbatchlatest"
PREFIX_LATEST = "rowdata/latest/"
PREFIX_OLD = "rowdata/old/"

# PostgreSQL Config
PG_HOST = "postgres"
PG_PORT = 5432
PG_USER = "admin"
PG_PASSWORD = "password"
PG_DATABASE = "gtfs_batch"


# Notebooks paths inside the Airflow container
NOTEBOOK_EXTRACT = "/opt/airflow/dags/notebooks/gtfs_extract_workspace/gtfs_extract_and_load_minio.ipynb"
OUTPUT_EXTRACT = "/opt/airflow/dags/notebooks/gtfs_extract_workspace/gtfs_extract_and_load_minio_output.ipynb"

NOTEBOOK_LOAD = "/opt/airflow/dags/notebooks/gtfs_load_to_postgres/gtfs_load_to_postgres.ipynb"
OUTPUT_LOAD = "/opt/airflow/dags/notebooks/gtfs_load_to_postgres/gtfs_load_to_postgres_output.ipynb"

# SQL Scripts paths inside the Airflow container
REFRESH_SQL_FILE = "/opt/airflow/dags/sql/gtfs_refresh_foreign_table.sql"
SCD2_SQL_FILE = "/opt/airflow/dags/sql/gtfs_scd2_script.sql"

# MinIO utils
def get_minio_client():
    return Minio(
        MINIO_ENDPOINT,
        access_key=MINIO_ACCESS_KEY,
        secret_key=MINIO_SECRET_KEY,
        secure=False
    )

def ensure_bucket_exists(client):
    if not client.bucket_exists(BUCKET_NAME):
        client.make_bucket(BUCKET_NAME)
        print(f"Bucket '{BUCKET_NAME}' created.")
    else:
        print(f"Bucket '{BUCKET_NAME}' already exists.")

# Tasks
def run_extract_notebook():
    pm.execute_notebook(NOTEBOOK_EXTRACT, OUTPUT_EXTRACT, parameters={})

def check_minio_data():
    client = get_minio_client()
    ensure_bucket_exists(client)
    objects = list(client.list_objects(BUCKET_NAME, PREFIX_LATEST, recursive=True))
    if not objects:
        raise AirflowSkipException("No data found in MinIO path, skipping load step.")

def run_load_notebook():
    pm.execute_notebook(NOTEBOOK_LOAD, OUTPUT_LOAD, parameters={})

def move_minio_data():
    client = get_minio_client()
    ensure_bucket_exists(client)
    objects = list(client.list_objects(BUCKET_NAME, PREFIX_LATEST, recursive=True))
    if not objects:
        print("No files to move from latest to old.")
        return
    for obj in objects:
        old_key = obj.object_name.replace(PREFIX_LATEST, PREFIX_OLD, 1)
        client.copy_object(
            BUCKET_NAME,
            old_key,
            CopySource(BUCKET_NAME, obj.object_name)
        )
        client.remove_object(BUCKET_NAME, obj.object_name)
    print(f"Moved {len(objects)} objects from {PREFIX_LATEST} to {PREFIX_OLD}")

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


# DAG
with DAG(
    dag_id="gtfs_full_pipeline_dag",
    start_date=datetime(2025, 8, 11),
    #schedule_interval=None,
    schedule_interval="0 0 * * *",
    catchup=False,
    tags=["gtfs", "MinIO", "Web_Scraping", "Spark", "Postgres", "SCD2", "Notebook"],
) as dag:

    extract_task = PythonOperator(
        task_id="extract_and_load_minio",
        python_callable=run_extract_notebook
    )

    check_data_task = PythonOperator(
        task_id="check_minio_latest_data",
        python_callable=check_minio_data
    )

    load_task = PythonOperator(
        task_id="load_to_stg_postgres_table",
        python_callable=run_load_notebook
    )

    move_data_task = PythonOperator(
        task_id="move_data_in_minio",
        python_callable=move_minio_data
    )

    refresh_foreign_table_task = PythonOperator(
        task_id="refresh_foreign_stg_postgres_table",
        python_callable=lambda: run_sql_from_file(REFRESH_SQL_FILE)
    )

    run_scd2_task = PythonOperator(
        task_id="run_scd2_script",
        python_callable=lambda: run_sql_from_file(SCD2_SQL_FILE)
    )

    extract_task >> check_data_task >> load_task >> move_data_task >> refresh_foreign_table_task >> run_scd2_task
