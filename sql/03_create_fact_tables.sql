CREATE TABLE IF NOT EXISTS fact_orders (
    order_id VARCHAR(50) PRIMARY KEY,
    customer_id INT REFERENCES dim_customers(customer_id),
    product_id INT REFERENCES dim_products(product_id),
    quantity INT,
    unit_price NUMERIC(10,2),
    total_amount NUMERIC(12,2),
    payment_method VARCHAR(30),
    payment_status VARCHAR(30),
    order_status VARCHAR(30),
    order_created_at TIMESTAMPTZ,
    order_updated_at TIMESTAMPTZ
);

CREATE TABLE IF NOT EXISTS fact_payments (
    payment_event_id VARCHAR(100) PRIMARY KEY,
    order_id VARCHAR(50),
    customer_id INT,
    amount NUMERIC(12,2),
    payment_method VARCHAR(30),
    payment_status VARCHAR(30),
    payment_timestamp TIMESTAMPTZ
);