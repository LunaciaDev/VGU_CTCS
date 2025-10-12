# For the extraction, we need 3 steps
# Get the timestamp from Postgres
# Parse the table in MSSQL for changes
# Load changes, if any, into Postgres
# Update the timestamp in Postgres
# COMMIT change.
# It is imperative that we DO NOT COMMIT before updating the timestamp.
# That to ensure atomicity of operation

from os import getenv
from dotenv import load_dotenv

# Configurations
load_dotenv()
MSSQL_HOSTNAME = "localhost"
MSSQL_DB = "CompanyX"
MSSQL_APP_ACC = getenv("MSSQL_APP_ACC")
MSSQL_APP_PASS = getenv("MSSQL_APP_PASS")
POSTGRES_APP_ACC = getenv("POSTGRES_APP_ACC")
POSTGRES_APP_PASS = getenv("POSTGRES_APP_PASS")

def load_tables():
    pass

if __name__ == "__main__":
    pass