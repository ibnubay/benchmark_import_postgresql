CREATE TABLE IF NOT EXISTS sales (
    region VARCHAR,
    country VARCHAR,
    item_type VARCHAR,
    sales_channel VARCHAR,
    order_priority VARCHAR,
    order_date DATE,
    order_id BIGINT,
    ship_date DATE,
    unit_sold INT,
    unit_price NUMERIC(10,2),
    unit_cost NUMERIC(10,2),
    total_revenue NUMERIC(15,2),
    total_cost NUMERIC(15,2),
    total_profit NUMERIC(15,2)
);