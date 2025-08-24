from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import papermill as pm
import clickhouse_connect

# ======================================== Config DB ========================================
# ClickHouse Config
CLICKHOUSE_HOST = "clickhouse"
CLICKHOUSE_USER = "default"
CLICKHOUSE_PASSWORD = "123"
CLICKHOUSE_SQL_FILE = "/opt/airflow/dags/sql/gtfs_drop_duplication_clickhouse.sql"

# ======================================== Paths ========================================
# Paths inside the Airflow container
NOTEBOOK_PATH = "/opt/airflow/dags/notebooks/gtfs_transfer_data_to_clickhouse/gtfs_transfer_data_to_clickhouse.ipynb"
OUTPUT_PATH = "/opt/airflow/dags/notebooks/gtfs_transfer_data_to_clickhouse/gtfs_transfer_data_to_clickhouse_output.ipynb"


# ======================================== Tasks ========================================
def run_notebook():
    pm.execute_notebook(
        NOTEBOOK_PATH,
        OUTPUT_PATH,
        parameters={}
    )

def run_sql_from_file(sql_file_path):
    with open(sql_file_path, "r") as file:
        sql_script = file.read()

    print("🚀 Running ClickHouse init SQL...")
    client = clickhouse_connect.get_client(
        host=CLICKHOUSE_HOST,
        user=CLICKHOUSE_USER,
        password=CLICKHOUSE_PASSWORD,
    )

    commands = [cmd.strip() for cmd in sql_script.split(";") if cmd.strip()]

    for command in commands:
        print(f"📤 Executing:\n{command}")
        client.command(command)

    print("✅ ClickHouse Drop Duplication script executed.")

# ======================================== DAG ========================================
with DAG(
    dag_id="gtfs_transfer_data_to_clickhouse_dag",
    start_date=datetime(2025, 8, 11),
    schedule_interval=None,
    #schedule_interval="0 0 * * *",
    catchup=False,
    tags=["gtfs", "Spark", "Postgres", "ClickHouse", "Notebook", "SQL"],
) as dag:

    run_notebook_task = PythonOperator(
        task_id="run_gtfs_transfer_data_to_clickhouse_notebook",
        python_callable=run_notebook
    )

    run_sql_script_task = PythonOperator(
        task_id="run_drop_duplication_script",
        python_callable=lambda: run_sql_from_file(CLICKHOUSE_SQL_FILE)
    )

    run_notebook_task >> run_sql_script_task
