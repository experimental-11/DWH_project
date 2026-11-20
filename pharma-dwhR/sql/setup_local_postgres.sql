-- Run ONCE in pgAdmin (Query Tool on the "postgres" database) or psql, as the postgres superuser.
-- Creates the user and database the pipeline uses.
CREATE ROLE dwh WITH LOGIN PASSWORD 'dwh';
CREATE DATABASE pharma_dwh OWNER dwh;
