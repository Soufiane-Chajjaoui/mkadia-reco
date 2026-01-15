#!/bin/bash

set -e

echo "Initializing Airflow..."

export AIRFLOW__CORE__EXECUTOR=LocalExecutor
export AIRFLOW__CORE__SQL_ALCHEMY_CONN=postgresql+psycopg2://postgres:soufianch@postgres:5432/airflow_db
export AIRFLOW__CORE__DAGS_FOLDER=/opt/airflow/dags
export AIRFLOW__CORE__LOAD_EXAMPLES=False
export AIRFLOW__CORE__LOAD_DEFAULT_CONNECTIONS=False
export AIRFLOW_UID=50000

echo "Creating airflow_db database if not exists..."
PGPASSWORD=soufianch psql -h postgres -U postgres -tc "SELECT 1 FROM pg_database WHERE datname = 'airflow_db'" | grep -q 1 || \
PGPASSWORD=soufianch psql -h postgres -U postgres -c "CREATE DATABASE airflow_db;" || echo "Database already exists"

echo "Initializing Airflow database..."
airflow db init

echo "Creating default user..."
airflow users create \
    --username admin \
    --firstname Admin \
    --lastname User \
    --role Admin \
    --email admin@mkadia.com \
    --password admin123 || echo "User already exists"

echo "Airflow initialization complete!"
echo "WebUI available at: http://localhost:8080"
echo "Credentials: admin / admin123"
