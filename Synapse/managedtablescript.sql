CREATE TABLE dbo.ordersdata_managed
(
    order_id       VARCHAR(20),
    customer_id    VARCHAR(20),
    product_id     VARCHAR(20),
    order_date     DATE,
    quantity       INT,
    total_amount   DECIMAL(10,2),
    region         VARCHAR(50)
)
WITH (
    DISTRIBUTION = HASH(product_id),
    CLUSTERED COLUMNSTORE INDEX
);


INSERT INTO dbo.ordersdata_managed
VALUES 
('ORD010', 'CUST010', 'PROD005', '2023-09-14', 1, 29.99, 'North');


CREATE TABLE dbo.productdata_managed 
(
    product_id     VARCHAR(20),
    product_name   VARCHAR(100),
    category       VARCHAR(50)
)
WITH (
    DISTRIBUTION = REPLICATE,
    CLUSTERED COLUMNSTORE INDEX
);



INSERT INTO dbo.productdata_managed (product_id, product_name, category)
VALUES 
('PROD020', 'Wireless Charger', 'Electronics')

select * from dbo.ordersdata_managed
select * from dbo.productdata_managed 



/*low performance query */
WITH joined_data AS (
    SELECT 
        o.*, 
        p.product_name, 
        p.category
    FROM dbo.ordersdata_managed o
    JOIN dbo.productdata_managed p
        ON o.product_id = p.product_id
),

category_month_sales AS (
    SELECT 
        FORMAT(order_date, 'yyyy-MM') AS month_bucket,
        region,
        category,
        SUM(total_amount) AS revenue,
        SUM(quantity) AS total_quantity,
        COUNT(DISTINCT customer_id) AS customer_count,
        RANK() OVER (
            PARTITION BY region, FORMAT(order_date, 'yyyy-MM') 
            ORDER BY SUM(total_amount) DESC
        ) AS rank_in_month
    FROM joined_data
    GROUP BY region, FORMAT(order_date, 'yyyy-MM'), category
)

SELECT *
FROM category_month_sales
WHERE rank_in_month <= 3;

/*high performance query */

-- Step 1: Filter and pre-process early
WITH filtered_orders AS (
    SELECT 
        product_id,
        customer_id,
        region,
        FORMAT(order_date, 'yyyy-MM') AS month_bucket,
        quantity,
        total_amount
    FROM dbo.ordersdata_managed
    WHERE order_date >= '2023-01-01'
),

-- Step 2: Join with replicated table (broadcast join)
joined_orders AS (
    SELECT 
        o.region,
        o.month_bucket,
        p.category,
        o.customer_id,
        o.quantity,
        o.total_amount
    FROM filtered_orders o
    JOIN dbo.productdata_managed p
        ON o.product_id = p.product_id
),

-- Step 3: Aggregate
aggregated_data AS (
    SELECT 
        region,
        month_bucket,
        category,
        SUM(total_amount) AS revenue,
        SUM(quantity) AS total_quantity,
        COUNT(DISTINCT customer_id) AS customer_count
    FROM joined_orders
    GROUP BY region, month_bucket, category
),

-- Step 4: Rank
ranked_data AS (
    SELECT *,
        RANK() OVER (
            PARTITION BY region, month_bucket
            ORDER BY revenue DESC
        ) AS rank_in_month
    FROM aggregated_data
)

-- Final selection
SELECT 
    region,
    month_bucket,
    category,
    revenue,
    total_quantity,
    customer_count
FROM ranked_data
WHERE rank_in_month <= 3
ORDER BY region, month_bucket, revenue DESC;





CREATE TABLE dbo.watermark_tracking (
    table_name NVARCHAR(100) PRIMARY KEY,
    last_watermark_value DATETIME
);

-- Insert initial value
INSERT INTO dbo.watermark_tracking (table_name, last_watermark_value)
VALUES ('ordersdata', '2023-01-01 00:00:00');



