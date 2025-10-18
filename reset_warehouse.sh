#!/bin/bash

source .env

sudo sudo -u postgres psql -c "DROP DATABASE companyxwarehouse;"
sudo sudo -u postgres psql -c "CREATE DATABASE companyxwarehouse;"
sudo sudo -u postgres psql -c "GRANT CONNECT ON DATABASE companyxwarehouse TO $POSTGRES_APP_ACC;"
sudo sudo -u postgres psql -d companyxwarehouse -c "\
    GRANT USAGE ON SCHEMA public TO $POSTGRES_APP_ACC;
    ALTER DEFAULT PRIVILEGES IN SCHEMA public
        GRANT SELECT, INSERT, UPDATE, DELETE ON TABLES TO $POSTGRES_APP_ACC;"
sudo sudo -u postgres psql -d companyxwarehouse --file=warehouse_schema.sql
