-- Runs once, automatically, the first time the postgres container starts.
-- Airflow keeps its own metadata in a separate database so it never mixes with the warehouse.
CREATE ROLE airflow WITH LOGIN PASSWORD 'airflow';
CREATE DATABASE airflow OWNER airflow;
