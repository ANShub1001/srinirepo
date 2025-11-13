WITH joined_data AS (
    SELECT
        o.order_id,
        o.customer_id,
        o.product_id,
        p.product_name,
        p.category,
        o.order_date,
        o.quantity,
        o.total_amount,
        o.region,
        FORMAT(o.order_date, 'yyyy-MM') AS month_bucket
    FROM dbo.ordersdata_external o
    JOIN dbo.productdata_external p
        ON o.product_id = p.product_id
),

category_summary AS (
    SELECT
        region,
        month_bucket,
        category,
        COUNT(DISTINCT customer_id) AS customer_count,
        SUM(total_amount) AS total_sales,
        SUM(quantity) AS total_units,
        SUM(total_amount) / NULLIF(SUM(quantity), 0) AS avg_order_value,
        RANK() OVER (
            PARTITION BY region, month_bucket
            ORDER BY SUM(total_amount) DESC
        ) AS sales_rank
    FROM joined_data
    WHERE order_date >= '2023-01-01'
    GROUP BY region, month_bucket, category
)

SELECT *
FROM category_summary
WHERE sales_rank <= 2
ORDER BY region, month_bucket, total_sales DESC;
