CREATE TABLE Sales_Fact_Partitioned (
    SaleID INT,
    CustomerID INT,
    ProductID INT,
    SaleAmount FLOAT,
    SaleDate DATE
)
WITH (
    DISTRIBUTION = HASH(CustomerID),
    CLUSTERED COLUMNSTORE INDEX,
    PARTITION (SaleDate RANGE RIGHT FOR VALUES (
        '2023-01-01', '2023-02-01', '2023-03-01', '2023-04-01'
    ))
);

INSERT INTO Sales_Fact (SaleID, CustomerID, ProductID, SaleAmount, SaleDate) VALUES
(1, 101, 201, 250.50, '2022-12-31');
INSERT INTO Sales_Fact (SaleID, CustomerID, ProductID, SaleAmount, SaleDate) VALUES
(2, 102, 202, 120.00, '2023-01-15');
INSERT INTO Sales_Fact (SaleID, CustomerID, ProductID, SaleAmount, SaleDate) VALUES
(3, 103, 203, 330.75, '2023-02-20');
INSERT INTO Sales_Fact (SaleID, CustomerID, ProductID, SaleAmount, SaleDate) VALUES
(4, 104, 204, 560.00, '2023-03-10');
INSERT INTO Sales_Fact(SaleID, CustomerID, ProductID, SaleAmount, SaleDate) VALUES

(5, 105, 205, 85.99, '2023-04-05');
INSERT INTO Sales_Fact (SaleID, CustomerID, ProductID, SaleAmount, SaleDate) VALUES

(6, 106, 206, 610.30, '2023-04-15'); 


SELECT *
FROM dbo.Sales_Fact_Partitioned
WHERE SaleDate >= '2023-01-01' AND SaleDate < '2023-02-01';

--Partition Pruning: Synapse will only scan the partition for January 2023 and skip others (February, March, etc.).


SELECT SUM(SaleAmount) AS TotalSalesAmount
FROM dbo.Sales_Fact_Partitioned
WHERE SaleDate >= '2023-01-01' AND SaleDate < '2023-03-01'; 

--Partition Pruning: Synapse will scan only the January and February partitions 
--(i.e., partitions with data between the specified range) and skip the others. 

--INDEXES

CREATE TABLE Sales_Fact (
    SaleID INT,
    CustomerID INT,
    ProductID INT,
    SaleAmount FLOAT,
    SaleDate DATE
)
WITH (
    DISTRIBUTION = HASH(CustomerID),
    CLUSTERED COLUMNSTORE INDEX
);

CREATE NONCLUSTERED  INDEX IDX_Sales_CustomerID
ON Sales_Fact (CustomerID);

--Benefit: This index improves query performance when looking up records by SaleID for specific rows.

CREATE STATISTICS Sales_Fact_CustomerID
ON Sales_Fact (CustomerID);

--Benefit: The query optimizer can use the statistics on CustomerID to estimate the 
--cardinality of the result set during query execution and improve the efficiency of queries that filter by CustomerID.

UPDATE STATISTICS Sales_Fact;

SELECT * 
FROM sys.stats
WHERE object_id = OBJECT_ID('dbo.Sales_Fact');
--Benefit: This query shows you the current statistics on the Sales_Fact table, 
--which can be useful for performance tuning.

SELECT SaleID, SaleAmount, SaleDate
FROM Sales_Fact
WHERE CustomerID = 101;
--Benefit: The query will use the non-clustered index on CustomerID to quickly filter the records, 
--rather than scanning the entire table.

SELECT CustomerID, SUM(SaleAmount) AS TotalSales
FROM Sales_Fact
GROUP BY CustomerID;
--Benefit: The optimizer can use statistics on CustomerID to understand the distribution of values, 
--improving the performance of the grouping operation. 


--Workload Management 

CREATE WORKLOAD GROUP etl_group
WITH (
    MIN_PERCENTAGE_RESOURCE = 20,
    CAP_PERCENTAGE_RESOURCE = 30,
    REQUEST_MIN_RESOURCE_GRANT_PERCENT = 10  
);

CREATE WORKLOAD CLASSIFIER pbi_classifier
WITH (
    WORKLOAD_GROUP = 'etl_group',
    MEMBERNAME = 'powerbi_user',
    IMPORTANCE = HIGH
);




-- Then create user in your Synapse SQL Pool
CREATE USER powerbi_user WITHOUT LOGIN 

-- Grant necessary permissions


GRANT SELECT ON 

CREATE ROLE powerbi_readonly; 

GRANT SELECT ON dbo.Sales_Fact TO powerbi_readonly;

ALTER ROLE powerbi_readonly ADD MEMBER powerbi_user;

exec sp_addrolemember 'powerbi_readonly','powerbi_user' 


SELECT * FROM sys.workload_management_workload_groups;
SELECT * FROM sys.workload_management_workload_classifiers;




--Analyzing Execution Plans
EXPLAIN
SELECT CustomerID, SUM(SaleAmount) AS TotalSales
FROM Sales_Fact
GROUP BY CustomerID;

--Output: JSON plan describing steps like HashJoin, DataMovement, Scan, etc. 


--DMV Query Track running or past queries:
SELECT *
FROM sys.dm_pdw_exec_requests
ORDER BY start_time DESC; 

SELECT * 
FROM sys.dm_pdw_request_steps 
WHERE request_id = 'QID123...';
--This shows which steps consumed the most CPU, IO, and time.

 --Caching Strategies in Synapse
ALTER DATABASE [srinidedicatedsqlpool]
SET RESULT_SET_CACHING ON; 

SET RESULT_SET_CACHING ON;

SELECT CustomerID, SUM(SaleAmount) AS TotalSales
FROM Sales_Fact
GROUP BY CustomerID; 

SELECT CustomerID, SUM(SaleAmount) AS TotalSales
FROM Sales_Fact
GROUP BY CustomerID;  


SELECT name, is_result_set_caching_on
FROM sys.databases;


CREATE MATERIALIZED VIEW mv_CustomerSales  
WITH  
(  
    DISTRIBUTION = HASH(CustomerID),  
    CLUSTERED COLUMNSTORE INDEX  
)  
AS  
SELECT  
    CustomerID,  
    SUM(SaleAmount) AS TotalSpent,  
    COUNT_BIG(SaleAmount) AS SaleCount
FROM dbo.Sales_Fact  
GROUP BY CustomerID;

select * from mv_CustomerSales 



CREATE TABLE dbo.Orders_Fact1
(
    order_id      VARCHAR(100)      NOT NULL,
    customer_id   VARCHAR(100)      NOT NULL,
    product_id    VARCHAR(100)      NOT NULL,
    quantity      INT               NOT NULL,
    order_date    DATE              NOT NULL,
    total_amount  DECIMAL(18,2)     NOT NULL,
    region          VARCHAR(50)     NULL
)
WITH
(
    DISTRIBUTION = HASH(customer_id),           -- Choose a column with high cardinality
    CLUSTERED COLUMNSTORE INDEX                 -- For analytical performance
);


COPY INTO dbo.Orders_Fact
FROM 'https://sriniadls123.dfs.core.windows.net/srinicontainer/demofolder/ordersdata1.csv'
WITH (
    FILE_TYPE = 'CSV',
    CREDENTIAL = (IDENTITY = 'Managed Identity'),
    FIELDTERMINATOR = ',',
    ROWTERMINATOR = '0x0A',
    FIRSTROW = 2,
    MAXERRORS = 0
);

select * from dbo.Orders_Fact




