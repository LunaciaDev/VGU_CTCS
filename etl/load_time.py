import calendar
import datetime
import psycopg
import pymssql

LOAD_TIME_SQL = """
SELECT DISTINCT
    OrderMonth = DATEPART(month, header.OrderDate),
    OrderYear = DATEPART(year, header.OrderDate)
FROM Sales.SalesOrderHeader AS header
WHERE header.Status != 6 AND header.CustomerID IN (
    SELECT c.CustomerID
    FROM Person.Person AS p
        JOIN Sales.Customer AS c ON c.PersonID = p.BusinessEntityID
    WHERE p.PersonType = 'IN'
)
ORDER BY OrderYear, OrderMonth
"""
LOAD_TIME_INC_SQL = """
SELECT DISTINCT
    OrderMonth = DATEPART(month, header.OrderDate),
    OrderYear = DATEPART(year, header.OrderDate),
    ModifiedDate = header.ModifiedDate
FROM Sales.SalesOrderHeader AS header
WHERE header.Status != 6 AND header.CustomerID IN (
    SELECT c.CustomerID
    FROM Person.Person AS p
        JOIN Sales.Customer AS c ON c.PersonID = p.BusinessEntityID
    WHERE p.PersonType = 'IN'
)
AND header.ModifiedDate > %(time)s
ORDER BY OrderYear, OrderMonth
"""

def load_time_initial(ms_cur: pymssql.Cursor, pg_cur: psycopg.Cursor):
    ms_cur.execute(LOAD_TIME_SQL)
    results = ms_cur.fetchall()

    with pg_cur.copy(
        "COPY dimtime (timekey, \"Day\", \"Month\", \"Year\") FROM STDIN"
    ) as copy:
        for row in results:
            day = calendar.monthrange(row[1], row[0])[1]
            key = row[1] * 10000 + row[0] * 100 + day
            copy.write_row((key, day, row[0], row[1]))


def load_time_incremental(
    ms_cur: pymssql.Cursor, pg_cur: psycopg.Cursor, timestamp: datetime
) -> datetime:
    ms_cur.execute(LOAD_TIME_INC_SQL, {"time": timestamp})
    results = ms_cur.fetchall()

    if len(results) == 0 or results is None:
        return
    
    max_modified_date = datetime.datetime.min

    # Cannot use copy in incremental loading due to the fact that the column may be duplicated.
    for row in results:
        day = calendar.monthrange(row[1], row[0])[1]
        key = row[1] * 10000 + row[0] * 100 + day

        max_modified_date = max(max_modified_date, row[2])

        if pg_cur.execute("SELECT * FROM dimtime AS d WHERE d.timekey = %s", key).fetchone() is None:
            pg_cur.execute("INSERT INTO dimtime (timekey, \"Day\", \"Month\", \"Year\") VALUES (%s, %s, %s, %s)", (key, day, row[0], row[1]))

    # Return the largest timestamp found for this dimension
    return max_modified_date