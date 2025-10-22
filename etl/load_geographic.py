import datetime
import psycopg
import pymssql

LOAD_GEOGRAPHIC_SQL = """
SELECT DISTINCT
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
WHERE person.BusinessEntityID IN (
    SELECT p.BusinessEntityID
    FROM Person.Person AS p
        JOIN Sales.Customer AS c ON c.PersonID = p.BusinessEntityID
    WHERE p.PersonType = 'IN'
)
"""

LOAD_GEOGRAPHIC_SQL_INC = """
SELECT DISTINCT
    CityName = address_data.City,
    StateName = state_data.Name,
    CountryName = country_data.Name,
    TerritoryName = territory_data.Name,
    paModifiedDate = person_address.ModifiedDate,
    adModifiedDate = address_data.ModifiedDate,
    sdModifiedDate = state_data.ModifiedDate,
    tdModifiedDate = territory_data.ModifiedDate
FROM Person.Person AS person
    JOIN Person.BusinessEntityAddress AS person_address ON person.BusinessEntityID = person_address.BusinessEntityID
    JOIN Person.Address AS address_data ON person_address.AddressID = address_data.AddressID
    JOIN Person.StateProvince AS state_data ON state_data.StateProvinceID = address_data.StateProvinceID
    JOIN Person.CountryRegion AS country_data ON state_data.CountryRegionCode = country_data.CountryRegionCode
    JOIN Sales.SalesTerritory AS territory_data ON state_data.TerritoryID = territory_data.TerritoryID
WHERE person.BusinessEntityID IN (
    SELECT p.BusinessEntityID
    FROM Person.Person AS p
        JOIN Sales.Customer AS c ON c.PersonID = p.BusinessEntityID
    WHERE p.PersonType = 'IN'
)
AND (person_address.ModifiedDate > %(time)s OR address_data.ModifiedDate > %(time)s OR state_data.ModifiedDate > %(time)s OR territory_data.ModifiedDate > %(time)s)
"""


def load_geographic_initial(ms_cur: pymssql.Cursor, pg_cur: psycopg.Cursor):
    ms_cur.execute(LOAD_GEOGRAPHIC_SQL)
    results = ms_cur.fetchall()

    with pg_cur.copy(
        "COPY dimgeographic (cityname, stateprovincename, countryregionname, territoryname) FROM STDIN"
    ) as copy:
        for row in results:
            copy.write_row(row)


def load_geographic_incremental(
    ms_cur: pymssql.Cursor, pg_cur: psycopg.Cursor, timestamp: datetime
) -> datetime:
    ms_cur.execute(LOAD_GEOGRAPHIC_SQL_INC, {"time": timestamp})
    results = ms_cur.fetchall()

    if len(results) == 0 or results is None:
        return

    max_timestamp = datetime.datetime.min

    # Cannot use copy in incremental loading due to the fact that the column may be duplicated.
    for row in results:
        max_timestamp = max(max_timestamp, row[4], row[5], row[6], row[7])

        if (
            pg_cur.execute(
                "SELECT d.geographickey FROM dimgeographic AS d WHERE d.cityname = %s AND d.stateprovincename = %s AND d.countryregionname = %s AND d.territoryname = %s",
                row,
            ).fetchone()
            is None
        ):
            pg_cur.execute(
                "INSERT INTO dimgeographic (cityname, stateprovincename, countryregionname, territoryname) VALUES (%s, %s, %s, %s)",
                row,
            )

    # Return the largest timestamp of this dimension
    return max_timestamp
