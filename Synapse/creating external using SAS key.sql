

CREATE MASTER KEY ENCRYPTION BY PASSWORD = '@srinu1001'
go

CREATE DATABASE SCOPED CREDENTIAL ADLS_srinicred
WITH IDENTITY='SHARED ACCESS SIGNATURE',  
SECRET = 'sv=2024-11-04&ss=bfqt&srt=sco&sp=rwdlacupyx&se=2025-04-09T20:28:51Z&st=2025-04-09T12:28:51Z&spr=https&sig=pJuyadGIKHyVDyNBA%2BKdRn%2BOOBqdbY0Bgu04SL1Bupc%3D'


CREATE EXTERNAL DATA SOURCE srinidemods2
WITH
  -- Please note the abfss endpoint when your account has secure transfer enabled
  ( LOCATION = 'abfss://demo@srinistracc.dfs.core.windows.net' ,
    ) ;

CREATE EXTERNAL DATA SOURCE YellowTaxi
WITH ( LOCATION = 'https://srinistracc.blob.core.windows.net/demo/',
    )


CREATE EXTERNAL FILE FORMAT sriniMyCsvFormat3
WITH (
    FORMAT_TYPE = DELIMITEDTEXT,
    FORMAT_OPTIONS (
        FIELD_TERMINATOR = ',',
        STRING_DELIMITER = '"',
        FIRST_ROW = 2
    )
);

CREATE EXTERNAL TABLE dbo.CustomerData15 (
    Id INT,
    name NVARCHAR(50),

)
WITH(
    LOCATION = 'Demo/sampleAZ.csv',
    DATA_SOURCE = srinidemods2,
    FILE_FORMAT = sriniMyCsvFormat2
)

select * from CustomerData15
