-- Step 1: Filter early and bucket
WITH filtered_orders AS (
    SELECT 
        order_id,
        customer_id,
        product_id,
        FORMAT(order_date, 'yyyy-MM') AS month_bucket,
        quantity,
        total_amount,
        region
    FROM dbo.ordersdata_external
    WHERE order_date >= '2023-01-01'
),

-- Step 2: Join with replicated dimension table
joined_data AS (
    SELECT
        o.customer_id,
        o.product_id,
        o.month_bucket,
        o.quantity,
        o.total_amount,
        o.region,
        p.category
    FROM filtered_orders o
    INNER JOIN dbo.productdata_external p
        ON o.product_id = p.product_id
),

-- Step 3: Aggregate once
aggregated_data AS (
    SELECT
        region,
        month_bucket,
        category,
        COUNT(DISTINCT customer_id) AS customer_count,
        SUM(total_amount) AS total_sales,
        SUM(quantity) AS total_units,
        SUM(total_amount) / NULLIF(SUM(quantity), 0) AS avg_order_value
    FROM joined_data
    GROUP BY region, month_bucket, category
),

-- Step 4: Rank on pre-aggregated result
ranked_summary AS (
    SELECT *,
        RANK() OVER (
            PARTITION BY region, month_bucket
            ORDER BY total_sales DESC
        ) AS sales_rank
    FROM aggregated_data
)

-- Step 5: Return top 2 categories
SELECT
    region,
    month_bucket,
    category,
    customer_count,
    total_units,
    total_sales,
    avg_order_value
FROM ranked_summary
WHERE sales_rank <= 2
ORDER BY region, month_bucket, total_sales DESC;
