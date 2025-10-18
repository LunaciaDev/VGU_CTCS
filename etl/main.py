import calendar
from collections import namedtuple
from datetime import date
from decimal import Decimal
from os import getenv
import re
from dotenv import load_dotenv
import psycopg
import pymssql
import xml.etree.ElementTree as ET
from math import ceil

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

CustomerData = namedtuple(
    "CustomerData", ["customerid", "name", "gender", "email_promotion_type"]
)


def get_dim_customer(
    customer_id: int, business_entity_id: int, cur: pymssql.Cursor
) -> CustomerData:
    cur.execute(
        """SELECT p.FirstName, p.MiddleName, p.LastName, p.Suffix, p.Demographics, p.EmailPromotion
            FROM Person.Person AS p
            WHERE p.BusinessEntityID = %d""",
        (business_entity_id,),
    )

    # CustomerID is unique
    row = cur.fetchone()

    # Construct data
    root = ET.fromstring(row["Demographics"])
    match = NAMESPACE_MATCHER.match(root.tag)
    namespace = match.group(1) if match is not None else ""
    gender = root.find(f"{{{namespace}}}Gender")

    return CustomerData(
        customerid=customer_id,
        gender=gender.text if gender is not None else None,
        name=" ".join(
            filter(
                None,
                [
                    row["FirstName"],
                    row["MiddleName"],
                    row["LastName"],
                    row["Suffix"],
                ],
            )
        ),
        email_promotion_type=row["EmailPromotion"],
    )


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


def get_dim_demographic(
    business_entity_id: int, cur: pymssql.Cursor
) -> DemographicData:
    cur.execute(
        """SELECT p.Demographics
            FROM Person.Person AS p
            WHERE p.BusinessEntityID = %d""",
        (business_entity_id,),
    )

    # CustomerID is unique
    row = cur.fetchone()

    # Construct data
    root = ET.fromstring(row["Demographics"])
    match = NAMESPACE_MATCHER.match(root.tag)
    namespace = match.group(1) if match is not None else ""

    # [TODO]: What if parse fails? Right now this thing assume all data is filled.

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


GeographicData = namedtuple(
    "GeographicData",
    ["city_name", "state_province_name", "country_region_name", "territory_name"],
)


def get_dim_geographic(business_entity_id: int, cur: pymssql.Cursor) -> GeographicData:
    cur.execute(
        """SELECT a.City, s.Name as StateProvince, c.Name as Country, t.Name as Territory
            FROM Person.BusinessEntityAddress AS i
            JOIN Person.Address AS a ON i.AddressID = a.AddressID
            JOIN Person.StateProvince AS s ON s.StateProvinceID = a.StateProvinceID
            JOIN Person.CountryRegion AS c ON c.CountryRegionCode = s.CountryRegionCode
            JOIN Sales.SalesTerritory AS t ON s.TerritoryID = t.TerritoryID
            WHERE i.BusinessEntityID = %s""",
        (business_entity_id,),
    )

    row = cur.fetchone()

    return GeographicData(
        city_name=row["City"],
        state_province_name=row["StateProvince"],
        country_region_name=row["Country"],
        territory_name=row["Territory"],
    )


def month_iterator(start: date, end: date):
    """
    Iterator that yield date object from start to end.
    Each time, yield +1 month:
    ie. April 2022, May 2022, June 2022, ...
    """

    current = date(start.year, start.month, 1)
    current = current.replace(day=calendar.monthrange(current.year, current.month)[1])
    final = date(end.year, end.month, 1)
    final = final.replace(day=calendar.monthrange(final.year, final.month)[1])

    while current <= final:
        yield current

        if current.month == 12:
            current = date(current.year + 1, 1, 1)
            current = current.replace(day=calendar.monthrange(current.year, current.month)[1])

        else:
            current = date(current.year, current.month + 1, 1)
            current = current.replace(day=calendar.monthrange(current.year, current.month)[1])


def update_database(
    customer_data: CustomerData,
    demographic_data: DemographicData,
    geographic_data: GeographicData,
    customer_id: int,
    pg_cur: psycopg.Cursor,
    mssql_cur: pymssql.Cursor,
):
    current_date = date.today()
    # Assemble foreign keys
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

    # Fetch Sales Data
    mssql_cur.execute(
        """SELECT OrderMonth = DATEPART(month, s.OrderDate),
        OrderYear = DATEPART(year, s.OrderDate),
        MonthTotal = SUM(s.SubTotal),
        MonthCount = COUNT(s.SalesOrderID),
        MaxTime = MAX(s.OrderDate)
    FROM Sales.Customer AS c
    JOIN Sales.SalesOrderHeader AS s ON s.CustomerID = c.CustomerID
    WHERE s.Status != 6 AND c.CustomerID = 11019
    GROUP BY DATEPART(year, s.OrderDate), DATEPART(month, s.OrderDate)
    ORDER BY OrderYear, OrderMonth""",
        (customer_id,),
    )

    rows = iter(mssql_cur.fetchall())
    filled_data: dict[date, (int, int, int)] = {}
    entry = next(rows)

    first_purchase_report = date(entry["OrderYear"], entry["OrderMonth"], 1)
    first_purchase_report = first_purchase_report.replace(day=calendar.monthrange(
        first_purchase_report.year, first_purchase_report.month
    )[1])

    for key in month_iterator(first_purchase_report, current_date):
        if entry is None:
            filled_data[key] = (1, 1, 1)
            continue

        if entry["OrderMonth"] == key.month and entry["OrderYear"] == key.year:
            month_total = entry["MonthTotal"]
            month_count = entry["MonthCount"]
            max_time: date = entry["MaxTime"].date()

            recency_raw_score = (
                calendar.monthrange(key.year, key.month)[1] - max_time.day
            )
            recency_score = 5 - max((recency_raw_score - 1) // 7, 0)

            frequency_score = (
                month_count + 1 if month_count < 3 else (4 if month_count < 5 else 5)
            )

            monetary_raw_score = month_total / Decimal(month_count)
            monetary_score = (
                5
                if monetary_raw_score >= 3000
                else (
                    4
                    if monetary_raw_score >= 2000
                    else (
                        3
                        if monetary_raw_score >= 1000
                        else 2 if monetary_raw_score >= 300 else 1
                    )
                )
            )

            filled_data[key] = (recency_score, frequency_score, monetary_score)
            entry = next(rows, None)
            continue

        filled_data[key] = (1, 1, 1)

    # We filled in all report from when they started buying to now
    # Time to slam all of that into the warehouse
    # Cannot do COPY since we need DateKey

    for key, value in filled_data.items():
        date_fk = key.year * 10000 + key.month * 100 + key.day
        pg_cur.execute(
            """INSERT INTO dimtime (timekey, "Day", "Month", "Year")
            VALUES (%s, %s, %s, %s)
            ON CONFLICT (timekey) DO NOTHING""",
            (
                date_fk,
                key.day,
                key.month,
                key.year,
            ),
        )

        pg_cur.execute(
            """INSERT INTO factcustomermonthlysnapshot (customerkey, snapshotdatekey, demographickey, geographickey, segmentkey, recency_score, frequency_score, monetary_score)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)""",
            (
                customer_data_fk,
                date_fk,
                demographic_data_fk,
                geographic_data_fk,
                None,
                value[0],
                value[1],
                value[2],
            ),
        )


def initial_load(batch_id: int):
    # We do it in batch of 50 customers.
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
                ) as ps_conn:
                    with ps_conn.cursor() as ps_cur:
                        for row in rows:
                            customer_id = row["CustomerID"]
                            business_entity_id = row["BusinessEntityID"]

                            batch_id = customer_id

                            # Let's do construction!
                            # Fetch the data
                            customer_data = get_dim_customer(
                                customer_id, business_entity_id, mssql_cur
                            )
                            demographic_data = get_dim_demographic(
                                business_entity_id, mssql_cur
                            )
                            geographic_data = get_dim_geographic(
                                business_entity_id, mssql_cur
                            )

                            update_database(
                                customer_data,
                                demographic_data,
                                geographic_data,
                                customer_id,
                                ps_cur,
                                mssql_cur,
                            )
                    ps_conn.commit()


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
