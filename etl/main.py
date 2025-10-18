import calendar
from collections import namedtuple
from datetime import date, datetime
from decimal import Decimal
from os import getenv
import re
from dotenv import load_dotenv
import psycopg
import pymssql
import xml.etree.ElementTree as ET

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

NAMESPACE_MATCHER = re.compile(r"\{(.*)\}")

DemographicData = namedtuple(
    "DemographicData",
    [
        "marital_status",
        "age_band",
        "yearly_income_level",
        "number_cars_owned",
        "education",
        "occupation",
        "is_home_owner",
    ],
)

CustomerData = namedtuple(
    "CustomerData", ["customerid", "name", "gender", "email_promotion_type"]
)

GeographicData = namedtuple(
    "GeographicData",
    ["city_name", "state_province_name", "country_region_name", "territory_name"],
)


def generate_date_key(pg_cur: psycopg.Cursor, ms_cur: pymssql.Cursor):
    generator_sql = """
    SELECT DISTINCT TOP 1
        Month = DATEPART(month, header.OrderDate),
        Year = DATEPART(year, header.OrderDate)
    FROM Sales.SalesOrderHeader AS header
    ORDER BY Year, Month
    """

    ms_cur.execute(generator_sql)
    data = ms_cur.fetchone()
    start = [data["Month"], data["Year"]]
    end = (date.today().month, date.today().year)

    with pg_cur.copy(
        """COPY dimtime (timekey, "Day", "Month", "Year") FROM STDIN"""
    ) as copy:
        while start[0] <= end[0] or start[1] <= end[1]:
            day = calendar.monthrange(start[1], start[0])[1]
            copy.write_row(
                (start[1] * 10000 + start[0] * 100 + day, day, start[0], start[1])
            )

            if start[0] == 12:
                start[0] = 1
                start[1] += 1
                continue

            start[0] += 1


def generate_demographics(root: ET.Element, namespace: str) -> DemographicData:
    marital_status = root.find(f"{{{namespace}}}MaritalStatus").text
    birth_date = root.find(f"{{{namespace}}}BirthDate").text
    yearly_income_level = root.find(f"{{{namespace}}}YearlyIncome").text
    number_cars_owned = root.find(f"{{{namespace}}}NumberCarsOwned").text
    education = root.find(f"{{{namespace}}}Education").text
    occupation = root.find(f"{{{namespace}}}Occupation").text
    is_home_owner = root.find(f"{{{namespace}}}HomeOwnerFlag").text
    current_time = date.today()
    is_leap_year = lambda a: (a % 4 == 0 and a % 100 != 0) or a % 400 == 0

    # Calculate age band
    birth_date = date.fromisoformat(birth_date[:-1])
    # shift 29/02 up if it is not a leap year right now
    if (
        birth_date.day == 29
        and birth_date.month == 2
        and not is_leap_year(current_time.year)
    ):
        birth_date.day = 1
        birth_date.month = 3

    age = (
        current_time.year - birth_date.year + 1
        if current_time.month > birth_date.month
        or (
            current_time.month == birth_date.month
            and current_time.day >= birth_date.day
        )
        else 0
    )
    age_band = "<26" if age < 26 else ">60" if age > 60 else "26-60"

    # Booleanize is_home_owner
    is_home_owner = is_home_owner == "1"

    # Ranging number_cars_owned
    number_cars_owned = int(number_cars_owned)
    number_cars_owned = (
        "0" if number_cars_owned == 0 else "3+" if number_cars_owned >= 3 else "1-2"
    )

    return DemographicData(
        marital_status=marital_status,
        age_band=age_band,
        yearly_income_level=yearly_income_level,
        number_cars_owned=number_cars_owned,
        education=education,
        occupation=occupation,
        is_home_owner=is_home_owner,
    )


def load_customer(
    pg_cur: psycopg.Cursor, ms_cur: pymssql.Cursor, customer_id: int, entity_id: int
):
    dimensions_sql = """
    SELECT
        FirstName = person.FirstName,
        MiddleName = person.MiddleName,
        LastName = person.LastName,
        Suffix = person.Suffix,
        Demographics = person.Demographics,
        EmailPromotion = person.EmailPromotion,
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
    WHERE person.BusinessEntityID = %s
    """

    sales_aggregate_sql = """
    WITH StartMonth AS (
        SELECT 
            DATEADD(MONTH, DATEDIFF(MONTH, 0, MIN(OrderDate)), 0) AS MinMonthStart
        FROM Sales.SalesOrderHeader
    ),
    Numbers AS (
        SELECT TOP (DATEDIFF(MONTH, (SELECT MinMonthStart FROM StartMonth), GETDATE()) + 1) 
            ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) - 1 AS n
        FROM sys.all_objects
    ),
    Months AS (
        SELECT 
            YEAR(DATEADD(MONTH, n, (SELECT MinMonthStart FROM StartMonth))) AS OrderYear,
            MONTH(DATEADD(MONTH, n, (SELECT MinMonthStart FROM StartMonth))) AS OrderMonth
        FROM Numbers
    ),
    CustomerMonths AS (
        SELECT
            c.CustomerID,
            m.OrderYear,
            m.OrderMonth
        FROM Sales.Customer AS c
        CROSS JOIN Months m
        WHERE c.CustomerID = %s
    )
    SELECT
        cm.CustomerID,
        cm.OrderYear,
        cm.OrderMonth,
        ISNULL(SUM(s.SubTotal), 0) AS MonthTotal,
        COUNT(s.SalesOrderID) AS MonthCount,
        MAX(s.OrderDate) AS MaxTime
    FROM CustomerMonths cm
    LEFT JOIN Sales.SalesOrderHeader s
        ON s.CustomerID = cm.CustomerID
        AND DATEPART(year, s.OrderDate) = cm.OrderYear
        AND DATEPART(month, s.OrderDate) = cm.OrderMonth
        AND s.Status != 6
    GROUP BY
        cm.CustomerID,
        cm.OrderYear,
        cm.OrderMonth
    ORDER BY
        cm.OrderYear,
        cm.OrderMonth,
        cm.CustomerID
    """

    # First, assemble the data
    ms_cur.execute(dimensions_sql, (entity_id,))
    data = ms_cur.fetchone()  # BEID is unique

    root = ET.fromstring(data["Demographics"])
    match = NAMESPACE_MATCHER.match(root.tag)
    namespace = match.group(1) if match is not None else ""

    ## Demographics
    demographic_data = generate_demographics(root, namespace)

    ## Customer
    gender = root.find(f"{{{namespace}}}Gender")
    customer_data = CustomerData(
        customerid=customer_id,
        gender=gender.text if gender is not None else None,
        name=" ".join(
            filter(
                None,
                [
                    data["FirstName"],
                    data["MiddleName"],
                    data["LastName"],
                    data["Suffix"],
                ],
            )
        ),
        email_promotion_type=data["EmailPromotion"],
    )

    ## Geographic
    geographic_data = GeographicData(
        city_name=data["CityName"],
        state_province_name=data["StateName"],
        country_region_name=data["CountryName"],
        territory_name=data["TerritoryName"],
    )

    # Now, get the surrogate keys
    # It's SIMPLE~ It first do a select to check if the row exist
    # If not, an INSERT is performed that immediately return the surrogate key.
    customer_data_fk = pg_cur.execute(
        """SELECT d.customerkey
        FROM dimcustomer AS d
        WHERE d.customerid = %s AND d.name = %s AND d.gender = %s AND d.emailpromotiontype = %s""",
        customer_data,
    ).fetchone()
    if customer_data_fk is None:
        customer_data_fk = pg_cur.execute(
            """INSERT INTO dimcustomer (customerid, name, gender, emailpromotiontype)
            VALUES (%s, %s, %s, %s)
            RETURNING customerkey""",
            customer_data,
        ).fetchone()[0]
    else:
        customer_data_fk = customer_data_fk[0]

    demographic_data_fk = pg_cur.execute(
        """SELECT d.demographickey
        FROM dimdemographic AS d
        WHERE d.maritalstatus = %s AND d.ageband = %s AND d.yearlyincomelevel = %s AND d.numbercarsowned = %s AND d.education = %s AND d.occupation = %s AND d.ishomeowner = %s""",
        demographic_data,
    ).fetchone()
    if demographic_data_fk is None:
        demographic_data_fk = pg_cur.execute(
            """INSERT INTO dimdemographic (maritalstatus, ageband, yearlyincomelevel, numbercarsowned, education, occupation, ishomeowner)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                RETURNING demographickey""",
            demographic_data,
        ).fetchone()[0]
    else:
        demographic_data_fk = demographic_data_fk[0]

    geographic_data_fk = pg_cur.execute(
        """SELECT d.geographickey
        FROM dimgeographic AS d
        WHERE d.cityname = %s AND d.stateprovincename = %s AND d.countryregionname = %s AND d.territoryname = %s""",
        geographic_data,
    ).fetchone()
    if geographic_data_fk is None:
        geographic_data_fk = pg_cur.execute(
            """INSERT INTO dimgeographic (cityname, stateprovincename, countryregionname, territoryname)
                VALUES (%s, %s, %s, %s)
                RETURNING geographickey""",
            geographic_data,
        ).fetchone()[0]
    else:
        geographic_data_fk = geographic_data_fk[0]

    ## Datetime fk has to be calculated for every row, so we cannot pregenerate

    # Finally, bulk data transfer
    ms_cur.execute(sales_aggregate_sql, (customer_id,))
    sales_data = ms_cur.fetchall()

    with pg_cur.copy(
        "COPY factcustomermonthlysnapshot (customerkey, snapshotdatekey, demographickey, geographickey, segmentkey, recency_score, frequency_score, monetary_score) FROM STDIN"
    ) as copy:
        for month in sales_data:
            # compute date_fk
            lastday_of_month = calendar.monthrange(
                month["OrderYear"], month["OrderMonth"]
            )[1]

            date_fk = (
                month["OrderYear"] * 10000
                + month["OrderMonth"] * 100
                + lastday_of_month
            )

            # Did we buy anything this month?
            if month["MaxTime"] is None:
                # Nope.
                copy.write_row((customer_data_fk, date_fk, demographic_data_fk, geographic_data_fk, None, 1, 1, 1))
                continue

            # compute the score
            raw_recency = lastday_of_month - month["MaxTime"].day
            raw_monetary = month["MonthTotal"] / month["MonthCount"]

            recency = 5 - max((raw_recency - 1) // 7, 0)
            frequency = (
                5 if month["MonthCount"] >= 5 else min(4, month["MonthCount"] + 1)
            )
            monetary = 1 if raw_monetary < 300 else min(2 + raw_monetary // 1000, 5)

            # load data
            copy.write_row(
                (
                    customer_data_fk,
                    date_fk,
                    demographic_data_fk,
                    geographic_data_fk,
                    None,
                    recency,
                    frequency,
                    monetary,
                )
            )


def initial_load(batch_id: int):
    # First, create the date dimension
    with psycopg.connect(
        f"host={POSTGRES_SERVER} port=5432 dbname={POSTGRES_DB} user={POSTGRES_APP_ACC} password={POSTGRES_APP_PASS}"
    ) as pg_conn:
        with pg_conn.cursor() as pg_cur:
            if pg_cur.execute("SELECT * FROM dimtime LIMIT 1").fetchone() is None:
                with pymssql.connect(
                    server="localhost",
                    user=MSSQL_APP_ACC,
                    password=MSSQL_APP_PASS,
                    database="CompanyX",
                ) as mssql_conn:
                    with mssql_conn.cursor(as_dict=True) as ms_cur:
                        generate_date_key(pg_cur, ms_cur)

    # Now, fill in the data, customer by customer.
    # We will commit every 500 customers
    while True:
        with pymssql.connect(
            server="localhost",
            user=MSSQL_APP_ACC,
            password=MSSQL_APP_PASS,
            database="CompanyX",
        ) as mssql_conn:
            with mssql_conn.cursor(as_dict=True) as mssql_cur:
                mssql_cur.execute(
                    """SELECT TOP 500 c.CustomerID, p.BusinessEntityID
                    FROM Person.Person AS p
                    JOIN Sales.Customer AS c ON c.PersonID = p.BusinessEntityID
                    WHERE c.CustomerID > %d AND p.PersonType = 'IN'
                    ORDER BY c.CustomerID""",
                    (batch_id,),
                )

                # Fetch all, so connection can be reused
                rows = mssql_cur.fetchall()

                print(batch_id)
                if rows is None:
                    return

                with psycopg.connect(
                    f"host={POSTGRES_SERVER} port=5432 dbname={POSTGRES_DB} user={POSTGRES_APP_ACC} password={POSTGRES_APP_PASS}"
                ) as pg_conn:
                    with pg_conn.cursor() as pg_cur:
                        # [TODO]: Somehow, we need to be able to batch more customer at once
                        # looping is way too slow.
                        for customer in rows:
                            customer_id = customer["CustomerID"]
                            entity_id = customer["BusinessEntityID"]

                            batch_id = customer_id

                            load_customer(pg_cur, mssql_cur, customer_id, entity_id)

                    pg_conn.commit()


def main():
    # Did we do the initial load?
    with psycopg.connect(
        f"host={POSTGRES_SERVER} dbname={POSTGRES_DB} user={POSTGRES_APP_ACC} password={POSTGRES_APP_PASS}"
    ) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT meta.BatchID, meta.FinishedLoad
                FROM ETLMeta_InitialLoad AS meta
            """
            )

            result = cur.fetchone()

    if result is None or not result[1]:
        initial_load(result[0] if result is not None else 0)


if __name__ == "__main__":
    main()
