-- Step 1: Create a credential using Managed Identity
CREATE DATABASE SCOPED CREDENTIAL ANSManagedIdentity
WITH IDENTITY = 'Managed Identity';
GO

-- Step 2: Create external data source
CREATE EXTERNAL DATA SOURCE ANS2AdlsSource
WITH (
    LOCATION = 'https://srinistracc.dfs.core.windows.net/demo',
    CREDENTIAL = ANSManagedIdentity
);
GO



CREATE EXTERNAL FILE FORMAT ANSParquetcompression
WITH
(  
    FORMAT_TYPE = PARQUET,
    DATA_COMPRESSION = 'org.apache.hadoop.io.compress.SnappyCodec'
)

-- Step 4: Create external table
CREATE EXTERNAL TABLE dbo.SalesSummary1
WITH (
    LOCATION = 'srinisynapsedemo/output1/',  -- make sure this is a folder path, not a file
    DATA_SOURCE = ANS1AdlsSource,
    FILE_FORMAT = ANSParquetcompression
)
AS
SELECT
    name,
    SUM(spentamount) AS SA

FROM
    OPENROWSET(
        BULK 'https://srinistracc.blob.core.windows.net/demo/Demo/customerspents.parquet',
        FORMAT = 'PARQUET'
    ) AS sales
GROUP BY name;



SELECT *
FROM OPENROWSET(
    BULK 'https://srinistracc.blob.core.windows.net/demo98765/srinisynapsedemo/output1/0C26DC16-1D8A-40A0-A388-4197A77E031F_20_0-1.parquet',
    FORMAT = 'PARQUET'
) AS [result];


