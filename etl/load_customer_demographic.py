import datetime
import re
import psycopg
import pymssql
import xml.etree.ElementTree as ET

CUSTOMER_DEMOGRAPHIC_SQL = """
SELECT
    CustomerID = customer.CustomerID,
    FirstName = person.FirstName,
    MiddleName = person.MiddleName,
    LastName = person.LastName,
    Suffix = person.Suffix,
    Demographics = person.Demographics,
    EmailPromotion = person.EmailPromotion,
    ModifiedDate = person.ModifiedDate
FROM Person.Person AS person
JOIN Sales.Customer AS customer ON person.BusinessEntityID = customer.PersonID
WHERE person.PersonType = 'IN'
"""
CUSTOMER_DEMOGRAPHIC_INC_SQL = """
SELECT
    CustomerID = customer.CustomerID,
    FirstName = person.FirstName,
    MiddleName = person.MiddleName,
    LastName = person.LastName,
    Suffix = person.Suffix,
    Demographics = person.Demographics,
    EmailPromotion = person.EmailPromotion
    ModifiedDate = person.ModifiedDate
FROM Person.Person AS person
JOIN Sales.Customer AS customer ON person.BusinessEntityID = customer.PersonID
WHERE person.ModifiedDate > %s AND person.PersonType = 'IN'
"""
NAMESPACE_MATCHER = re.compile(r"\{(.*)\}")

def parse_name_gender(row) -> tuple[str, str]:
    name = " ".join(filter(None, row[1:5]))
    root = ET.fromstring(row[5])
    match = NAMESPACE_MATCHER.match(root.tag)
    namespace = match.group(1) if match is not None else ""
    gender = root.find(f"{{{namespace}}}Gender")
    gender = gender.text if gender is not None else None

    return (name, gender)

def parse_demographic(xml) -> tuple:
    root = ET.fromstring(xml)
    match = NAMESPACE_MATCHER.match(root.tag)
    namespace = match.group(1) if match is not None else ""

    marital_status = root.find(f"{{{namespace}}}MaritalStatus").text
    birth_date = root.find(f"{{{namespace}}}BirthDate").text
    yearly_income_level = root.find(f"{{{namespace}}}YearlyIncome").text
    number_cars_owned = root.find(f"{{{namespace}}}NumberCarsOwned").text
    education = root.find(f"{{{namespace}}}Education").text
    occupation = root.find(f"{{{namespace}}}Occupation").text
    is_home_owner = root.find(f"{{{namespace}}}HomeOwnerFlag").text
    current_time = datetime.date.today()
    is_leap_year = lambda a: (a % 4 == 0 and a % 100 != 0) or a % 400 == 0

    # Calculate age band
    birth_date = datetime.date.fromisoformat(birth_date[:-1])
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

    return (
        marital_status,
        age_band,
        yearly_income_level,
        number_cars_owned,
        education,
        occupation,
        is_home_owner,
    )


def _load_customer_initial(pg_cur: psycopg.Cursor, data: list[tuple[any, ...]]):
    max_timestamp = datetime.datetime.min

    with pg_cur.copy(
        "COPY dimcustomer (customerid, name, gender, emailpromotiontype) FROM STDIN"
    ) as copy:
        for row in data:
            max_timestamp = max(max_timestamp, row[7])
            (name, gender) = parse_name_gender(row)
            copy.write_row((row[0], name, gender, row[6]))

    return max_timestamp

def _load_customer_incremental(pg_cur: psycopg.Cursor, data: list[tuple[any, ...]]):
    max_timestamp = datetime.datetime.min

    for row in data:
        max_timestamp = max(max_timestamp, row[7])
        (name, gender) = parse_name_gender(row)
        if (
            pg_cur.execute(
                "SELECT d.customerkey FROM dimcustomer AS d WHERE d.customerid = %s AND d.name = %s AND d.gender = %s AND d.emailpromotiontype = %s",
                (row[0], name, gender, row[6]),
            ).fetchone()
            is None
        ):
            pg_cur.execute(
                "INSERT INTO dimgeographic (cityname, stateprovincename, countryregionname, territoryname) VALUES (%s, %s, %s, %s)",
                (row[0], name, gender, row[6]),
            )

    return max_timestamp


def _load_demographic(pg_cur: psycopg.Cursor, data: list[tuple[any, ...]]):
    # Demographic cannot be copied since the data is not guaranteed distinct
    # Geographic can do since the SQL is SELECT DISTINCT
    # Same goes for time, and customer is guaranteed distinct due to source key constraint
    for row in data:
        demographic_data = parse_demographic(row[5])
        if (
            pg_cur.execute(
                "SELECT d.demographickey FROM dimdemographic AS d WHERE d.maritalstatus = %s AND d.ageband = %s AND d.yearlyincomelevel = %s AND d.numbercarsowned = %s AND d.education = %s AND d.occupation = %s AND d.ishomeowner = %s",
                demographic_data,
            ).fetchone()
            is None
        ):
            pg_cur.execute(
                "INSERT INTO dimdemographic (maritalstatus, ageband, yearlyincomelevel, numbercarsowned, education, occupation, ishomeowner) VALUES (%s, %s, %s, %s, %s, %s, %s)",
                demographic_data,
            )


def load_customer_demographic_initial(ms_cur: pymssql.Cursor, pg_cur: psycopg.Cursor):
    ms_cur.execute(CUSTOMER_DEMOGRAPHIC_SQL)
    data = ms_cur.fetchall()

    max_timestamp = _load_customer_initial(pg_cur, data)
    _load_demographic(pg_cur, data)

    return max_timestamp


def load_customer_demographic_incremental(
    ms_cur: pymssql.Cursor, pg_cur: psycopg.Cursor, timestamp: datetime.datetime
):
    ms_cur.execute(CUSTOMER_DEMOGRAPHIC_INC_SQL, (timestamp,))
    data = ms_cur.fetchall()

    if len(data) == 0 or data is None:
        return

    max_timestamp = _load_customer_incremental(pg_cur, data)
    _load_demographic(pg_cur, data)

    return max_timestamp
