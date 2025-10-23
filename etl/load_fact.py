import calendar
from datetime import datetime, date
import itertools
from logging import getLogger
import re
import psycopg
import pymssql

from load_customer_demographic import parse_demographic

START_DATE_SQL = """
SELECT DISTINCT
    OrderYear = DATEPART(year, header.OrderDate),
    OrderMonth = DATEPART(month, header.OrderDate)
FROM Sales.SalesOrderHeader AS header
WHERE header.ModifiedDate > %s AND header.Status != 6
"""
TRANSACTION_SQL = """
WITH RawData AS (
    SELECT
        header.CustomerID,
        OrderYear = DATEPART(year, header.OrderDate),
        OrderMonth = DATEPART(month, header.OrderDate),
        ISNULL(SUM(header.SubTotal), 0) AS MonthTotal,
        COUNT(header.SalesOrderID) AS MonthCount,
        MAX(header.OrderDate) AS LatestOrderDate,
        MAX(header.ModifiedDate) AS LatestModifiedDate
    FROM Sales.SalesOrderHeader AS header
    WHERE header.Status != 6 AND header.ModifiedDate > %s AND header.CustomerID = %s
    GROUP BY header.CustomerID,
        DATEPART(year, header.OrderDate),
        DATEPART(month, header.OrderDate)
)
SELECT
    CustomerID = rd.CustomerID,
    OrderYear = rd.OrderYear,
    OrderMonth = rd.OrderMonth,
    OrderDay = DATEPART(day, EOMONTH(DATEFROMPARTS(rd.OrderYear, rd.OrderMonth, 1))),
    Recency = CASE
        WHEN rd.LatestOrderDate IS NULL THEN 1
        ELSE 5 - GREATEST((DATEPART(day, EOMONTH(DATEFROMPARTS(rd.OrderYear, rd.OrderMonth, 1))) - DATEPART(day, rd.LatestOrderDate) - 1) / 7, 0)
    END,
    Frequency = CASE
        WHEN rd.LatestOrderDate IS NULL THEN 1
        WHEN rd.MonthCount >= 5 THEN 5
        ELSE LEAST(4, rd.MonthCount + 1)
    END,
    Monetary = CASE
        WHEN rd.LatestOrderDate IS NULL THEN 1
        WHEN (rd.MonthTotal / rd.MonthCount) < 300 THEN 1
        ELSE LEAST(2 + CAST(rd.MonthTotal / rd.MonthCount AS INT) / 1000, 5)
    END,
    ModifiedDate = rd.LatestModifiedDate
FROM RawData AS rd
ORDER BY OrderYear, OrderMonth, CustomerID
"""
GEOGRAPHIC_SQL = """
SELECT
    CityName = address_data.City,
    StateName = state_data.Name,
    CountryName = country_data.Name,
    TerritoryName = territory_data.Name
FROM Person.Person AS person
    JOIN Person.BusinessEntityAddress AS person_address ON person.BusinessEntityID = person_address.BusinessEntityID
    JOIN Person.Address AS address_data ON person_address.AddressID = address_data.AddressID
    JOIN Person.StateProvince AS state_data ON state_data.StateProvinceID = address_data.StateProvinceID
    JOIN Person.CountryRegion AS country_data ON state_data.CountryRegionCode = country_data.CountryRegionCode
    JOIN Sales.SalesTerritory AS territory_data ON state_data.TerritoryID = territory_data.TerritoryID
WHERE person.BusinessEntityID = %s AND person_address.AddressTypeID = 2"""
DEMOGRAPHIC_SQL = """
SELECT
    Demographics = person.Demographics
FROM Person.Person AS person
WHERE person.BusinessEntityID = %s"""
START_DATE_SQL = """
SELECT DISTINCT TOP 1
    header.OrderDate
FROM Sales.SalesOrderHeader AS header
WHERE header.Status != 6 AND header.CustomerID = %s
ORDER BY header.OrderDate"""
logger = getLogger(__name__)

# Generate report for those month from scratch
# TODO: Generate a connection pool for this task and attempt to multithread.

def _month_iterator(start_date: date, end_date: date):
    start = [start_date.year, start_date.month]
    end = (end_date.year, end_date.month)

    # While we have not reached the end date
    while start[0] < end[0] or start[1] <= end[1]:
        # yield the current year and month
        yield start

        # increment
        start[0] += start[1] // 12
        start[1] = start[1] % 12 + 1

def _date_conversion(input_date: date):
    return date(
        input_date.year,
        input_date.month,
        calendar.monthrange(input_date.year, input_date.month)[1],
    )

def load_fact(
    ms_cur: pymssql.Cursor,
    pg_cur: psycopg.Cursor,
    pg_conn: psycopg.Connection,
    run_timestamp: date = date.today(),
    last_updated_timestamp = date(1753, 1, 1)
):
    max_update_timestamp = datetime.min
    previous_id = 0

    # Insert the current run_timestamp into the fact table, if not exist
    # This is only relevant if the business make no new order on the run_timestamp...
    parsed_run_timestamp = _date_conversion(run_timestamp)
    pg_cur.execute(
        """
        INSERT INTO dimtime (timekey, "Day", "Month", "Year")
        VALUES (%s, %s, %s, %s)
        ON CONFLICT (timekey) DO NOTHING""",
        (
            parsed_run_timestamp.year * 10000
            + parsed_run_timestamp.month * 100
            + parsed_run_timestamp.day,
            parsed_run_timestamp.day,
            parsed_run_timestamp.month,
            parsed_run_timestamp.year,
        ),
    )

    # Do we have previous data?
    pg_cur.execute(
        """
        SELECT d.batchid, d.loadingtimestamp
        FROM etlmeta_factload AS d"""
    )
    result = pg_cur.fetchone()
    if result[0] is not None:
        # We do have data from previous run, that might have not finished.
        # Pick up from that point.
        logger.info("Detected an incomplete load. This load will pick up from that point instead of starting from scratch.")
        previous_id = result[0]
        max_update_timestamp = result[1]

    # Get our list of customers.
    ms_cur.execute(
        """
        SELECT c.CustomerID, p.BusinessEntityID
        FROM Person.Person AS p
            JOIN Sales.Customer AS c ON p.BusinessEntityID = c.PersonID
        WHERE p.PersonType = 'IN' AND c.CustomerID > %s
        ORDER BY c.CustomerID""",
        (previous_id, )
    )
    customers = ms_cur.fetchall()

    logger.info("Loading customers...")
    current_batch_id = 0

    # Batching into group of 500 customers
    customers_batch_iter = itertools.batched(customers, 500)

    for customers_batch in customers_batch_iter:
        logger.info("Processing customers batch %s, loaded %s customers so far", current_batch_id, current_batch_id * 500)
        last_id = 0

        for customerID, businessEntityID in customers_batch:
            last_id = customerID

            # Do we have any transaction?
            ms_cur.execute(TRANSACTION_SQL, (last_updated_timestamp, customerID))
            transactions = ms_cur.fetchall()

            if len(transactions) == 0:
                continue

            # There are transaction, so let's prepare to add them.

            # Get FKs for new snapshots
            ms_cur.execute(GEOGRAPHIC_SQL, (businessEntityID,))
            pg_cur.execute(
                "SELECT d.geographickey FROM dimgeographic AS d WHERE d.cityname = %s AND d.stateprovincename = %s AND d.countryregionname = %s AND d.territoryname = %s",
                ms_cur.fetchone(),
            )
            geographic_fk = pg_cur.fetchone()[0]

            ms_cur.execute(DEMOGRAPHIC_SQL, (businessEntityID,))
            pg_cur.execute(
                "SELECT d.demographickey FROM dimdemographic AS d WHERE d.maritalstatus = %s AND d.ageband = %s AND d.yearlyincomelevel = %s AND d.numbercarsowned = %s AND d.education = %s AND d.occupation = %s AND d.ishomeowner = %s",
                parse_demographic(ms_cur.fetchone()[0]),
            )
            demographic_fk = pg_cur.fetchone()[0]

            pg_cur.execute(
                "SELECT d.customerkey FROM dimcustomer AS d WHERE d.customerid = %s",
                (customerID,),
            )
            customer_fk = pg_cur.fetchone()[0]

            # Generate snapshots for all month since their first purchase if it does not exist
            ms_cur.execute(START_DATE_SQL, (customerID,))
            result = ms_cur.fetchone()
            start_date = _date_conversion(result[0].date())

            for year, month in _month_iterator(start_date, parsed_run_timestamp):
                # Create the time key
                time_key = year * 10000 + month * 100 + calendar.monthrange(year, month)[1]
                # Attempt to insert default data, on conflict do nothing.
                pg_cur.execute(
                    """
                    INSERT INTO factcustomermonthlysnapshot (customerkey, snapshotdatekey, demographickey, geographickey, segmentkey, recency_score, frequency_score, monetary_score)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (customerkey, snapshotdatekey) DO NOTHING""",
                    (customer_fk, time_key, demographic_fk, geographic_fk, None, 1, 1, 1),
                )

            # Update months where the customer has updated header
            # DO NOT TOUCH demographic, geographic key. Just search by customer and snapshot date key.

            for entry in transactions:
                time_key = entry[1] * 10000 + entry[2] * 100 + entry[3]
                pg_cur.execute(
                    """
                    UPDATE factcustomermonthlysnapshot
                    SET recency_score = %s, frequency_score = %s, monetary_score = %s
                    WHERE customerkey = %s AND snapshotdatekey = %s""",
                    (entry[4], entry[5], entry[6], customer_fk, time_key),
                )

                max_update_timestamp = max(max_update_timestamp, entry[-1])

        # Finished loading this batch, we update the metadata and commit.
        pg_cur.execute("UPDATE etlmeta_factload SET batchid = %s, loadingtimestamp = %s", (last_id, max_update_timestamp))
        pg_conn.commit()
        current_batch_id += 1

    return max_update_timestamp
