CREATE TABLE IF NOT EXISTS DimTime (
  TimeKey     INTEGER PRIMARY KEY,        -- Surrogate key for date (e.g., 2025/01/31 -> 20250131)
  "Day"       SMALLINT NOT NULL CHECK ("Day" BETWEEN 1 AND 31),
  "Month"     SMALLINT NOT NULL CHECK ("Month" BETWEEN 1 AND 12),
  "Year"      INTEGER  NOT NULL
);

CREATE TABLE IF NOT EXISTS DimCustomer (
  CustomerKey        BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  CustomerID         INTEGER NOT NULL,                         -- NK from source
  Name               VARCHAR(120),
  Gender             CHAR(1) NULL CHECK (Gender IN ('M','F') OR Gender IS NULL),
  EmailPromotionType SMALLINT
);

CREATE TABLE IF NOT EXISTS DimDemographic (
  DemographicKey     BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  MaritalStatus      VARCHAR(20),
  AgeBand            VARCHAR(20),
  YearlyIncomeLevel  VARCHAR(30),
  NumberCarsOwned    VARCHAR(10),
  Education          VARCHAR(50),
  Occupation         VARCHAR(60),
  IsHomeOwner        BOOLEAN
);

CREATE TABLE IF NOT EXISTS DimGeographic (
  GeographicKey      BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  CityName           VARCHAR(60),
  StateProvinceName  VARCHAR(60),
  CountryRegionName  VARCHAR(60),
  TerritoryName      VARCHAR(60)
);

CREATE TABLE IF NOT EXISTS DimSegment (
  SegmentKey   BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  SegmentName  VARCHAR(40) NOT NULL UNIQUE
);

CREATE TABLE IF NOT EXISTS ETLMeta_TableTimestamp (
  TableKey INT PRIMARY KEY, -- Hardcoded Table ID
  ModifiedDate TIMESTAMP -- If this exists, the initial load is completed.
);

CREATE TABLE IF NOT EXISTS ETLMeta_FactLoad (
  ID INT PRIMARY KEY CHECK (ID = 1), -- This table must only have one single row.
  LoadFinished BOOLEAN, -- has we finished with the load?

  BatchID INT NULL, -- which customerID has we reached?
  LoadingTimestamp TIMESTAMP NULL -- Current largest fact timestamp
);

CREATE TABLE IF NOT EXISTS FactCustomerMonthlySnapshot (
  CustomerKey       BIGINT  NOT NULL,
  SnapshotDateKey   INTEGER NOT NULL,
  DemographicKey    BIGINT  NULL,
  GeographicKey     BIGINT  NULL,
  SegmentKey        BIGINT  NULL,

  Recency_Score     SMALLINT NOT NULL CHECK (Recency_Score   BETWEEN 1 AND 5),
  Frequency_Score   SMALLINT NOT NULL CHECK (Frequency_Score BETWEEN 1 AND 5),
  Monetary_Score    SMALLINT NOT NULL CHECK (Monetary_Score  BETWEEN 1 AND 5),

  CONSTRAINT PK_FactCustomerMonthlySnapshot
    PRIMARY KEY (CustomerKey, SnapshotDateKey),

  CONSTRAINT FK_Fact_Customer
    FOREIGN KEY (CustomerKey)
    REFERENCES DimCustomer(CustomerKey)
    ON UPDATE RESTRICT ON DELETE RESTRICT,

  CONSTRAINT FK_Fact_SnapshotDate
    FOREIGN KEY (SnapshotDateKey)
    REFERENCES DimTime(TimeKey)
    ON UPDATE RESTRICT ON DELETE RESTRICT,

  CONSTRAINT FK_Fact_Demographic
    FOREIGN KEY (DemographicKey)
    REFERENCES DimDemographic(DemographicKey)
    ON UPDATE RESTRICT ON DELETE SET NULL,

  CONSTRAINT FK_Fact_Geographic
    FOREIGN KEY (GeographicKey)
    REFERENCES DimGeographic(GeographicKey)
    ON UPDATE RESTRICT ON DELETE SET NULL,

  CONSTRAINT FK_Fact_Segment
    FOREIGN KEY (SegmentKey)
    REFERENCES DimSegment(SegmentKey)
    ON UPDATE RESTRICT ON DELETE SET NULL
);

-- Helpful indexes for analytics
CREATE INDEX IF NOT EXISTS IX_Fact_SnapshotDate
  ON FactCustomerMonthlySnapshot (SnapshotDateKey);
CREATE INDEX IF NOT EXISTS IX_Fact_Segment
  ON FactCustomerMonthlySnapshot (SegmentKey);
CREATE INDEX IF NOT EXISTS IX_Fact_Demographic
  ON FactCustomerMonthlySnapshot (DemographicKey);
CREATE INDEX IF NOT EXISTS IX_Fact_Geographic
  ON FactCustomerMonthlySnapshot (GeographicKey);