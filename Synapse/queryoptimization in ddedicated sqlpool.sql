WITH orders_filtered AS (
    SELECT 
        order_id,
        customer_id,
        product_id,
        order_date,
        quantity,
        total_amount,
        region,
        DATE_FORMAT(order_date, 'yyyy-MM') AS month_bucket
    FROM parquet.`abfss://demo@srinistracc.dfs.core.windows.net/optdemo/ordersdata.parquet`
    WHERE order_date BETWEEN '2024-01-01' AND '2024-12-31'
),

order_with_products AS (
    SELECT 
        o.order_id,
        o.customer_id,
        o.product_id,
        p.product_name,
        p.category,
        o.month_bucket,
        o.quantity,
        o.total_amount,
        o.region
    FROM orders_filtered o
    JOIN parquet.`abfss://demo@srinistracc.dfs.core.windows.net/optdemo/productdata.parquet` p
        ON o.product_id = p.product_id
),

product_sales_ranked AS (
    SELECT
        region,
        month_bucket,
        product_id,
        product_name,
        category,
        SUM(quantity) AS total_units_sold,
        SUM(total_amount) AS revenue,
        RANK() OVER (
            PARTITION BY region, month_bucket 
            ORDER BY SUM(total_amount) DESC
        ) AS revenue_rank
    FROM order_with_products
    GROUP BY region, month_bucket, product_id, product_name, category
)

SELECT
    region,
    month_bucket,
    product_id,
    product_name,
    category,
    total_units_sold,
    revenue
FROM product_sales_ranked
WHERE revenue_rank <= 3
ORDER BY region, month_bucket, revenue DESC
