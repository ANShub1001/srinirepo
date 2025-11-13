





-- Step 1: Create External Data Source
CREATE EXTERNAL DATA SOURCE sriniserverlesssqlpool
WITH (
    LOCATION = 'https://sriniadls123.dfs.core.windows.net/srinicontainer'
     );


-- Step 2: Create External File Format
CREATE EXTERNAL FILE FORMAT sriniserverlesssqlpoolff
WITH (
    FORMAT_TYPE = PARQUET,
   );

-- Step 3: Drop table if it exists
DROP EXTERNAL TABLE dbo.ordersdata1;

-- Step 4: Create External Table
CREATE EXTERNAL TABLE dbo.ordersdata (
    [order_id]        INT,
    [customer_id]     INT,
    [product_id]      INT,
    [order_date]      DATETIME,
    [quantity]        INT,
    [total_amount]    DECIMAL(10,2),
    [region]          VARCHAR(50)
)
WITH (
    LOCATION = 'demofolder',
    DATA_SOURCE = sriniserverlesssqlpool,
    FILE_FORMAT = sriniserverlesssqlpoolff
);


select * from dbo.ordersdata



SELECT TOP 10 * 
FROM OPENROWSET(
    BULK 'https://sriniadls123.dfs.core.windows.net/srinicontainer/demofolder/ordersdata.parquet',
    FORMAT='PARQUET'
) AS data;

DROP EXTERNAL TABLE IF EXISTS dbo.ordersdata_partitioned;

-- Unoptimized: Scans entire dataset and filters after reading
SELECT 
    region,
    product_id,
    COUNT(order_id) AS total_orders,
    SUM(total_amount) AS total_sales
FROM dbo.ordersdata
WHERE 
    order_date >= '2024-01-01' 
    AND order_date <= '2024-03-31'
GROUP BY 
    region, product_id
HAVING 
    COUNT(order_id) > 3;
--Problems:
--Scans all partitions, even unrelated regions/dates

--Doesn’t take advantage of folder-based partitioning

-- Optimized: Restricts by partition columns early (region, order_date)
SELECT 
    region,
    product_id,
    COUNT(order_id) AS total_orders,
    SUM(total_amount) AS total_sales
FROM dbo.ordersdata_partitioned
WHERE 
    region IN ('North', 'South', 'East') -- filtered partitions
    AND order_date BETWEEN '2024-01-01' AND '2024-03-31'
GROUP BY 
    region, product_id
HAVING 
    COUNT(order_id) > 3;


--Uses partition filters (region, order_date) → fewer folders scanned

--Reduces I/O, cost, and latency
