#!/bin/bash
# Runs once, automatically, the first time the postgres container starts (empty volume).
# Airflow keeps its own metadata in a separate database so it never mixes with the warehouse.
# The password comes from AIRFLOW_DB_PASSWORD in .env (never hard-coded here).
set -e
psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" <<EOSQL
CREATE ROLE airflow WITH LOGIN PASSWORD '${AIRFLOW_DB_PASSWORD}';
CREATE DATABASE airflow OWNER airflow;
EOSQL
