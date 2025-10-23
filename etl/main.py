from datetime import date
from logging import getLogger
import logging
from os import getenv
from dotenv import load_dotenv
import psycopg
import pymssql

from load_fact import load_fact
from load_customer_demographic import load_customer_demographic_initial
from load_geographic import load_geographic_initial
from load_time import load_time_initial

# Configurations
load_dotenv()
MSSQL_SERVER = "localhost"
MSSQL_DB = "CompanyX"
MSSQL_APP_ACC = getenv("MSSQL_APP_ACC")
MSSQL_APP_PASS = getenv("MSSQL_APP_PASS")
POSTGRES_SERVER = "localhost"
POSTGRES_DB = "companyxwarehouse"
POSTGRES_APP_ACC = getenv("POSTGRES_APP_ACC")
POSTGRES_APP_PASS = getenv("POSTGRES_APP_PASS")
TABLE_KEYS = {"time": 0, "geographic": 1, "customer_demographic": 2, "fact": 3}
logger = getLogger(__name__)
# Frankly speaking we do not emit anything but INFO, so.
logging.basicConfig(level=logging.INFO)

def _helper_initial_load_dimension(
    mssql_cur: pymssql.Cursor,
    pg_cur: psycopg.Cursor,
    pg_conn: psycopg.Connection,
    key_function_iterator,
):
    for key, function in key_function_iterator:
        logger.info("Attempting to load %s dimension", key)
        # Check if the dimension exists
        pg_cur.execute(
            "SELECT * FROM etlmeta_tabletimestamp AS t WHERE t.tablekey = %s",
            (TABLE_KEYS[key],),
        )
        result = pg_cur.fetchone()

        if result is None:
            # The dimension did not exist, so create it.
            logger.info("Dimension %s does not exist, loading.", key)
            timestamp = function(mssql_cur, pg_cur)
            pg_cur.execute(
                "INSERT INTO etlmeta_tabletimestamp (tablekey, modifieddate) VALUES (%s, %s)",
                (TABLE_KEYS[key], timestamp),
            )
            # Commit changes
            pg_conn.commit()
            logger.info("Finished loading dimension %s", key)
        else:
            logger.info("Dimension %s exists, skipping.", key)


def _initial_load(pg_conn: psycopg.Connection):
    with pymssql.connect(
        server="localhost",
        user=MSSQL_APP_ACC,
        password=MSSQL_APP_PASS,
        database="CompanyX",
    ) as mssql_conn:
        with mssql_conn.cursor() as mssql_cur:
            with pg_conn.cursor() as pg_cur:
                _helper_initial_load_dimension(
                    mssql_cur,
                    pg_cur,
                    pg_conn,
                    # Pair the key with the corresponding load function
                    zip(
                        ["time", "geographic", "customer_demographic"],
                        [
                            load_time_initial,
                            load_geographic_initial,
                            load_customer_demographic_initial,
                        ],
                    ),
                )

                logger.info("Finished loading dimensions. Loading facts.")

                # Dimension are loaded. Now we load the facts
                timestamp = load_fact(
                    ms_cur=mssql_cur, pg_cur=pg_cur, run_timestamp=date(2014, 7, 25)
                )
                # Load timestamp
                pg_cur.execute(
                    "INSERT INTO etlmeta_tabletimestamp (tablekey, modifieddate) VALUES (%s, %s)",
                    (TABLE_KEYS["fact"], timestamp),
                )
                # Mark initial load as finished
                pg_cur.execute(
                    "UPDATE etlmeta_initialload SET loadfinished = %s, incrementalbatchid = %s",
                    (True, 0),
                )
                pg_conn.commit()

                # done!
                logger.info("Finished loading facts.")


def _incremental_load(pg_conn: psycopg.Connection):
    pass


def main():
    # Check with the warehouse to see if we are doing initial load or incremental load.
    logger.info("Starting the ETL pipeline.")
    with psycopg.connect(
        f"host={POSTGRES_SERVER} port=5432 dbname={POSTGRES_DB} user={POSTGRES_APP_ACC} password={POSTGRES_APP_PASS}"
    ) as pg_conn:
        with pg_conn.cursor() as pg_cur:
            pg_cur.execute("SELECT * FROM etlmeta_initialload")
            result = pg_cur.fetchone()

            # If the row was not created
            if result is None:
                logger.info("Cannot detect previous initial load attempt, starting an initial load.")
                # Create the row, then start from scratch
                pg_cur.execute(
                    "INSERT INTO etlmeta_initialload (id, loadfinished, batchid) VALUES (%s, %s, %s)",
                    (1, False, 0),
                )
                _initial_load(pg_conn)
                logger.info("Initial load finished. Exiting.")
                return

            # If the initial load was marked incomplete
            if not result[1]:
                logger.info("Detected incomplete initial load attempt, resuming from that point.")
                _initial_load(pg_conn)
                logger.info("Initial load finished. Exiting.")
                return

            # The initial load succeeded, so this is an incremental load run
            logger.info("Detected a successful initial load attempt, running incremental load.")
            _incremental_load(pg_conn)
            logger.info("Incremental load finished. Exiting.")


if __name__ == "__main__":
    main()
