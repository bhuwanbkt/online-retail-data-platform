CREATE TABLE IF NOT EXISTS agg_daily_sales (
    sales_date DATE PRIMARY KEY,
    total_orders INT,
    total_revenue NUMERIC(14,2),
    total_failed_payments INT,
    avg_order_value NUMERIC(12,2)
);

CREATE TABLE IF NOT EXISTS agg_hourly_orders (
    sales_hour TIMESTAMP PRIMARY KEY,
    total_orders INT,
    total_revenue NUMERIC(14,2)
);

CREATE TABLE IF NOT EXISTS agg_top_products (
    snapshot_date DATE,
    product_id INT,
    product_name VARCHAR(150),
    category VARCHAR(100),
    total_quantity INT,
    total_revenue NUMERIC(14,2),
    PRIMARY KEY (snapshot_date, product_id)
);