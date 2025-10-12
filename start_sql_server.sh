#!/bin/bash

# Start the SQL Server, blocking until the server fully starts up.
source .env
sudo docker container start sql1
echo "SQL Server container started. Waiting for the server to fully warm up."
until sudo docker exec sql1 /opt/mssql-tools18/bin/sqlcmd \
                       -S localhost \
                       -U $MSSQL_APP_ACC -P $MSSQL_APP_PASS -No \
                       -Q "SELECT 1" > /dev/null 2>&1
do
    echo "Working on it..."
    sleep 1
done