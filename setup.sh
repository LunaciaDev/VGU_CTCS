#!/bin/bash

# Stop the script if any error happen
set -e

# Source the environment secrets
source .env

# Update the package list
sudo apt-get update

# --- Prepare SQL Server ---
sudo docker pull mcr.microsoft.com/mssql/server:2022-latest
sudo docker run -e "ACCEPT_EULA=Y" -e "MSSQL_SA_PASSWORD=Default_Password" \
            -p 1433:1433 --name sql1 --hostname sql1 \
            --mount type=tmpfs,dst=/tmp \
            --mount type=bind,src=$(realpath dataset),dst=/run/mount/dataset,ro \
            -d \
            mcr.microsoft.com/mssql/server:2022-latest

until sudo docker exec sql1 /opt/mssql-tools18/bin/sqlcmd \
                       -S localhost \
                       -U sa -P Default_Password -No \
                       -Q "SELECT 1" > /dev/null 2>&1
do
    echo "Waiting for SQL Server to start..."
    sleep 2
done

# SQL Server is ready to accept input.

# Remove sa account and replace it with env cred
sudo docker exec sql1 /opt/mssql-tools18/bin/sqlcmd \
            -S localhost \
            -U sa -P Default_Password -No \
            -Q "CREATE LOGIN $MSSQL_ROOT_ACC WITH PASSWORD='$MSSQL_ROOT_PASS';
                ALTER SERVER ROLE sysadmin ADD MEMBER $MSSQL_ROOT_ACC;
                ALTER LOGIN sa DISABLE;"

# Restore from backup
sudo docker exec sql1 /opt/mssql-tools18/bin/sqlcmd \
            -S localhost \
            -U $MSSQL_ROOT_ACC -P $MSSQL_ROOT_PASS -No \
            -Q "RESTORE DATABASE CompanyX
                FROM DISK='/run/mount/dataset/CompanyX.bak'
                WITH MOVE 'AdventureWorks2022' TO '/var/opt/mssql/data/AdventureWorks2022.mdf',
                     MOVE 'AdventureWorks2022_log' TO '/var/opt/mssql/data/AdventureWorks2022_log.ldf',
                     REPLACE"

# Create application account
sudo docker exec sql1 /opt/mssql-tools18/bin/sqlcmd \
            -S localhost \
            -U $MSSQL_ROOT_ACC -P $MSSQL_ROOT_PASS -No \
            -Q "CREATE LOGIN $MSSQL_APP_ACC WITH PASSWORD='$MSSQL_APP_PASS';
                USE CompanyX;
                CREATE USER $MSSQL_APP_ACC FOR LOGIN $MSSQL_APP_ACC;
                GRANT SELECT, INSERT, UPDATE, DELETE TO $MSSQL_APP_ACC"

# --- Finish Preparing SQL Server ---

# --- PostgreSQL ---

# Install postgres
sudo apt-get install postgresql --yes
sudo service postgresql start

# Prepare the database
sudo sudo -u postgres psql -c "ALTER USER postgres WITH PASSWORD '$POSTGRES_ROOT_PASS';"
sudo sudo -u postgres psql -c "CREATE DATABASE companyxwarehouse;"
sudo sudo -u postgres psql -c "CREATE USER $POSTGRES_APP_ACC WITH PASSWORD '$POSTGRES_APP_PASS';"
sudo sudo -u postgres psql -c "GRANT CONNECT ON DATABASE companyxwarehouse TO $POSTGRES_APP_ACC;"
sudo sudo -u postgres psql -d companyxwarehouse -c "\
    GRANT USAGE ON SCHEMA public TO $POSTGRES_APP_ACC;
    ALTER DEFAULT PRIVILEGES IN SCHEMA public
        GRANT SELECT, INSERT, UPDATE, DELETE ON TABLES TO $POSTGRES_APP_ACC;"

# [TODO]: Create the Star Schema inside the database
# sudo sudo -u postgres psql -d companyxwarehouse --file=warehouse_schema.sql

# --- Finish Preparing PostgreSQL ---

# --- Python ETL ---

python -m venv .venv/etl
source .venv/etl/bin/activate
pip install python-dotenv pymssql psycopg[binary] --no-input
deactivate

# --- Finish Python ETL

# Done with initial setup!

