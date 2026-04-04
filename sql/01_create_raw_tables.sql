CREATE TABLE IF NOT EXISTS raw_ecommerce_events (
    id BIGSERIAL PRIMARY KEY,
    event_id VARCHAR(100) UNIQUE NOT NULL,
    event_type VARCHAR(50) NOT NULL,
    order_id VARCHAR(50) NOT NULL,
    customer_id INT,
    product_id INT,
    quantity INT,
    unit_price NUMERIC(10,2),
    total_amount NUMERIC(12,2),
    payment_method VARCHAR(30),
    payment_status VARCHAR(30),
    order_status VARCHAR(30),
    customer_city VARCHAR(100),
    customer_state VARCHAR(50),
    event_timestamp TIMESTAMPTZ NOT NULL,
    ingestion_timestamp TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
    raw_payload JSONB NOT NULL
);