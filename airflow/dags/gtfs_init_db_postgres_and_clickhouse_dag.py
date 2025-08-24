from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import psycopg2
import clickhouse_connect

# PostgreSQL Config
PG_HOST = "postgres"
PG_PORT = 5432
PG_USER = "admin"
PG_PASSWORD = "password"
PG_DATABASE = "admin"
PG_SQL_FILE = "/opt/airflow/dags/sql/gtfs_init_postgres.sql"

# ClickHouse Config
CLICKHOUSE_HOST = "clickhouse"
CLICKHOUSE_USER = "default"
CLICKHOUSE_PASSWORD = "123"
CLICKHOUSE_SQL_FILE = "/opt/airflow/dags/sql/gtfs_init_clickhouse.sql"

dag = DAG(
    dag_id="gtfs_init_pipeline",
    start_date=datetime(2025, 8, 1),
    description="Create GTFS database in Postgres and GTFS tables in ClickHouse",
    schedule_interval=None,
    catchup=False,
    tags=["gtfs", "Postgres", "ClickHouse", "FDW"],
)

# ---------------------------------------------------------
# 🧱 TASK 1 - Create 'gtfs' databases in PostgreSQL
    
def create_postgres_databases():
    print("🚀 Connecting to PostgreSQL...")
    conn = psycopg2.connect(
        host=PG_HOST,
        port=PG_PORT,
        user=PG_USER,
        password=PG_PASSWORD,
        database=PG_DATABASE,
    )
    conn.autocommit = True
    cur = conn.cursor()

    # ------ Create gtfs_batch database if not exists ------
    cur.execute("SELECT 1 FROM pg_database WHERE datname = 'gtfs_batch';")
    exists = cur.fetchone()
    if not exists:
        print("📦 Creating database 'gtfs_batch'...")
        cur.execute("CREATE DATABASE gtfs_batch;")
        print("✅ Database 'gtfs_batch' created.")
    else:
        print("ℹ️ Database 'gtfs_batch' already exists.")

    # ------ Create gtfs_batch_staging database if not exists ------
    cur.execute("SELECT 1 FROM pg_database WHERE datname = 'gtfs_batch_staging';")
    exists = cur.fetchone()
    if not exists:
        print("📦 Creating database 'gtfs_batch_staging'...")
        cur.execute("CREATE DATABASE gtfs_batch_staging;")
        print("✅ Database 'gtfs_batch_staging' created.")
    else:
        print("ℹ️ Database 'gtfs_batch_staging' already exists.")

    # ------ Run Script FDW Creation ------
    # with open(FWD_SQL_FILE, "r") as file:
    #     sql_script = file.read()

    # cur.execute(sql_script)

    conn.commit()
    cur.close()
    conn.close()

# ---------------------------------------------------------
# 🧱 TASK 2 - Run Postgres SQL script

def run_sql_from_file(sql_file_path):
    with open(sql_file_path, "r") as file:
        sql_script = file.read()

    conn = psycopg2.connect(
        host=PG_HOST,
        port=PG_PORT,
        user=PG_USER,
        password=PG_PASSWORD,
        database="gtfs_batch",
    )
    conn.autocommit = True
    cur = conn.cursor()
    cur.execute(sql_script)
    conn.commit()
    cur.close()
    conn.close()

# ---------------------------------------------------------
# 🧱 TASK 3 - Run ClickHouse SQL script
def run_clickhouse_init():
    print("🚀 Running ClickHouse init SQL...")
    client = clickhouse_connect.get_client(
        host=CLICKHOUSE_HOST,
        user=CLICKHOUSE_USER,
        password=CLICKHOUSE_PASSWORD,
    )

    with open(CLICKHOUSE_SQL_FILE, "r") as f:
        sql_script = f.read()

    # Create Database for separate execution
    create_db_stmt = ""
    if "CREATE DATABASE" in sql_script:
        for line in sql_script.splitlines():
            if line.strip().startswith("CREATE DATABASE"):
                create_db_stmt = line.strip()
                break

    if create_db_stmt:
        print(f"📤 Executing database creation:\n{create_db_stmt}")
        client.command(create_db_stmt)

    # Remove any 'USE gtfs;' from the script
    sql_script = sql_script.replace("USE gtfs_batch;", "").replace("use gtfs_batch;", "")

    # Execute the remaining commands
    commands = [cmd.strip() for cmd in sql_script.split(";") if cmd.strip()]

    for command in commands:
        print(f"📤 Executing:\n{command}")
        client.command(command)

    print("✅ ClickHouse init script executed.")

# ---------------------------------------------------------
# 🛠️ Operators
create_pg_db_task = PythonOperator(
    task_id="create_postgres_gtfs_db",
    python_callable=create_postgres_databases,
    dag=dag,
)

pg_init_task = PythonOperator(
    task_id="run_postgres_sql_script",
    python_callable=lambda: run_sql_from_file(PG_SQL_FILE),
    dag=dag,
)

clickhouse_init_task = PythonOperator(
    task_id="run_clickhouse_sql_script",
    python_callable=run_clickhouse_init,
    dag=dag,
)

create_pg_db_task >> pg_init_task >> clickhouse_init_task
