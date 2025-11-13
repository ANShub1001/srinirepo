

-- Step 1: Create a master key (if not already done)
CREATE MASTER KEY;

-- Step 2: Create database scoped credential
CREATE DATABASE SCOPED CREDENTIAL ADLS_DEMO_CRED
WITH IDENTITY = 'Managed Identity';

-- Step 3: Create external data source
CREATE EXTERNAL DATA SOURCE AzureDataLakeDemo
WITH (
    LOCATION = 'abfss://demo@srinistracc.dfs.core.windows.net/',
    CREDENTIAL = ADLS_DEMO_CRED
);


CREATE EXTERNAL FILE FORMAT ParquetFileFormat
WITH (
    FORMAT_TYPE = PARQUET
);


CREATE EXTERNAL TABLE dbo.productdata_external1 (
    product_id     VARCHAR(10),
    product_name   VARCHAR(100),
    category       VARCHAR(50)
)
WITH (
    LOCATION = 'optdemo/productdata.parquet',
    DATA_SOURCE = AzureDataLakeDemo,
    FILE_FORMAT = ParquetFileFormat,
    
);


CREATE EXTERNAL TABLE dbo.ordersdata_external (
    order_id       VARCHAR(20),
    customer_id    VARCHAR(20),
    product_id     VARCHAR(20),
    order_date     DATE,
    quantity       INT,
    total_amount   DECIMAL(10,2),
    region         VARCHAR(20)
)
WITH (
    LOCATION = 'optdemo/ordersdata.parquet',
    DATA_SOURCE = AzureDataLakeDemo,
    FILE_FORMAT = ParquetFileFormat
);

select * from dbo.ordersdata_external


CREATE EXTERNAL TABLE dbo.ordersdata_external1 (
    order_id       VARCHAR(20),
    customer_id    VARCHAR(20),
    product_id     VARCHAR(20),
    order_date     DATE,
    quantity       INT,
    total_amount   DECIMAL(10,2),
    region         VARCHAR(20)
)
WITH (
    LOCATION = 'optdemo/ordersdata.parquet',
    DATA_SOURCE = AzureDataLakeDemo,
    FILE_FORMAT = ParquetFileFormat,
    REJECT_TYPE = VALUE,
    REJECT_VALUE = 0,
    DISTRIBUTION = HASH(product_id)
);



