
create database ANSdb

CREATE DATABASE SCOPED CREDENTIAL ANSManagedIdentity
WITH IDENTITY = 'Managed Identity';
GO


-- Step 1: Create External Data Source
CREATE EXTERNAL DATA SOURCE my_adls
WITH (
    LOCATION = 'https://sriniadls123.dfs.core.windows.net/srinicontainer'
     CREDENTIAL = ANSManagedIdentity
);


-- Step 2: Create External File Format
CREATE EXTERNAL FILE FORMAT ANSParquetcompression
WITH (
    FORMAT_TYPE = PARQUET,
    DATA_COMPRESSION = 'org.apache.hadoop.io.compress.SnappyCodec'
);

-- Step 3: Drop table if it exists
DROP EXTERNAL TABLE dbo.iot_messages1;

-- Step 4: Create External Table
CREATE EXTERNAL TABLE dbo.iot_messages1 (
    messageid     VARCHAR(250),
    device_id     VARCHAR(250),
    temperature   FLOAT,
    humidity      FLOAT,
    timestamp   DATETIME
)
WITH (
    LOCATION = 'ingested/eventhub/',
    DATA_SOURCE = my_adls,
    FILE_FORMAT = ANSParquetcompression
);


select * from dbo.iot_messages1

SELECT TOP 100 *
FROM OPENROWSET(
    BULK 'https://sriniadls123.dfs.core.windows.net/srinicontainer/ingested/eventhub/',
    FORMAT = 'PARQUET'
) AS rows;


















