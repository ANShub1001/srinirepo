CREATE TABLE dbo.ordersdata_managed_INC
(
    order_id      VARCHAR(20),
    customer_id   VARCHAR(20),
    product_id    VARCHAR(20),
    order_date    DATETIME,
    quantity      INT,
    total_amount  DECIMAL(10,2),
    region        VARCHAR(50)
)
WITH
(
    DISTRIBUTION = HASH(product_id),
    CLUSTERED COLUMNSTORE INDEX
);



CREATE TABLE dbo.watermark
(
    table_name     VARCHAR(100) NOT NULL,
    last_loaded_ts DATETIME     NOT NULL
)
WITH
(
    DISTRIBUTION = REPLICATE, -- small lookup-style table
    HEAP
);

 update dbo.watermark set table_name ='dbo.ordersdata_managed_INC ' where table_name = 'ordersdata_managed'
INSERT INTO dbo.watermark (table_name, last_loaded_ts)
VALUES ('ordersdata_managed', '2022-12-31 23:59:59'); 


INSERT INTO dbo.ordersdata_managed_INC (
    order_id,
    customer_id,
    product_id,
    order_date,
    quantity,
    total_amount,
    region
)
SELECT 
    order_id,
    customer_id,
    product_id,
    CAST(order_date AS DATETIME) AS order_date,
    quantity,
    total_amount,
    region
FROM dbo.ordersdata_managed; 


INSERT INTO dbo.ordersdata_managed_INC 
(order_id, customer_id, product_id, order_date, quantity, total_amount, region)
VALUES ('ORD101', 'CUST101', 'PROD001', '2023-01-15 10:30:00', 2, 49.98, 'East');


update dbo.ordersdata_managed_INC
 set quantity =3,total_amount =84.5,order_date='2023-02-05 14:20:00' where order_id ='ORD105' 
select * from watermark

INSERT INTO dbo.ordersdata_managed_INC 
(order_id, customer_id, product_id, order_date, quantity, total_amount, region)
VALUES ('ORD111', 'CUST102', 'PROD002', '2023-02-05 14:20:00', 1, 1.99, 'West');

INSERT INTO dbo.ordersdata_managed_INC 
(order_id, customer_id, product_id, order_date, quantity, total_amount, region)
VALUES ('ORD112', 'CUST103', 'PROD003', '2025-04-21 13:01:30', 5, 90.25, 'North');

INSERT INTO dbo.ordersdata_managed_INC_dest
(order_id, customer_id, product_id, order_date, quantity, total_amount, region)
VALUES ('ORD101', 'CUST101', 'PROD001', '2023-01-15 10:30:00', 2, 49.98, 'East');





select * from dbo.ordersdata_managed_INC_dest
 truncate table dbo.ordersdata_managed_INC