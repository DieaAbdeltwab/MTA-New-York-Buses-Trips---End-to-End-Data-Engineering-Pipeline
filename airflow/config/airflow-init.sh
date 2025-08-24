#!/bin/bash

# check if DB is already initialized
if [ ! -f /opt/airflow/.initialized ]; then
    echo "🔧 Initializing Airflow DB..."
    airflow db init

    echo "👤 Creating admin user..."
    airflow users create \
        --username admin \
        --firstname Admin \
        --lastname User \
        --role Admin \
        --email admin@example.com \
        --password admin

    touch /opt/airflow/.initialized
fi
#================================================
# PostgreSQL connection
airflow connections add 'postgres_conn' \
    --conn-type 'postgres' \
    --conn-login 'admin' \
    --conn-password 'password' \
    --conn-host 'postgres' \
    --conn-port '5432' \
    --conn-schema 'admin'
    
# MySQL connection
airflow connections add 'mysql_conn' \
    --conn-type 'mysql' \
    --conn-login 'myuser' \
    --conn-password 'mypassword' \
    --conn-host 'mysql' \
    --conn-port '3306' \
    --conn-schema 'mydb'

#================================================
echo "🚀 Starting Airflow Webserver..."
exec airflow webserver
#================================================

