create database ANSDB

CREATE EXTERNAL DATA SOURCE srinidemods
WITH
  -- Please note the abfss endpoint when your account has secure transfer enabled
  ( LOCATION = 'abfss://demo@srinistracc.dfs.core.windows.net' ,
    ) ;


CREATE EXTERNAL FILE FORMAT sriniMyCsvFormat
WITH (
    FORMAT_TYPE = DELIMITEDTEXT,
    FORMAT_OPTIONS (
        FIELD_TERMINATOR = ',',
        STRING_DELIMITER = '"',
        FIRST_ROW = 2
    )
);

CREATE EXTERNAL TABLE dbo.CustomerData12 (
    Id INT,
    name NVARCHAR(50),

)
WITH(
    LOCATION = 'Demo/sampleAZ.csv',
    DATA_SOURCE = srinidemods,
    FILE_FORMAT = sriniMyCsvFormat
)

select * from CustomerData12
