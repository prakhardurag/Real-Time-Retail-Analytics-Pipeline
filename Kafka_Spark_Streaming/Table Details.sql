-- CREATE STATEMENTS FOR ALL THE TABLES REQUIRED
CREATE TABLE dim_location (
    store_id     INTEGER PRIMARY KEY,
    city         VARCHAR(100),
    state        VARCHAR(100),
    country      VARCHAR(100) DEFAULT 'USA'
);

CREATE TABLE dim_products (
    product_id   INTEGER PRIMARY KEY,
    name         VARCHAR(255),
    category     VARCHAR(100),
    price        NUMERIC(10, 2)
);

CREATE TABLE facts_transaction (
    transaction_id VARCHAR(100) PRIMARY KEY,
    product_id INT REFERENCES dim_products(product_id),
    product_name VARCHAR(255),
    product_category VARCHAR(100),
    store_id INT REFERENCES dim_location(store_id),
    city VARCHAR(100),
    state VARCHAR(100),
    country VARCHAR(100),
    quantity INT,
    unit_price NUMERIC(10, 2),
    total_price NUMERIC(10, 2),
    payment_method VARCHAR(50),
    transaction_timestamp TIMESTAMP
);


CREATE TABLE IF NOT EXISTS bad_transactions (
    transaction_id VARCHAR,
    product_id INTEGER,
    store_id INTEGER,
    quantity INTEGER,
    price DOUBLE PRECISION,
    payment_method VARCHAR,
    timestamp TEXT
);

--INSERT STATEMENTS

INSERT INTO dim_location (store_id, city, state, country)
VALUES
    (1, 'New York', 'New York', 'USA'),
    (2, 'Los Angeles', 'California', 'USA'),
    (3, 'Chicago', 'Illinois', 'USA'),
    (4, 'Houston', 'Texas', 'USA'),
    (5, 'Phoenix', 'Arizona', 'USA');


INSERT INTO dim_products (product_id, name, category, price)
VALUES
    (101, 'iPhone 15', 'Electronics', 999.99),
    (102, 'Samsung QLED TV', 'Electronics', 1499.99),
    (103, 'PlayStation 5', 'Gaming', 499.99),
    (104, 'Dell XPS Laptop', 'Computers', 1299.99),
    (105, 'Sony Headphones', 'Audio', 199.99),
    (106, 'Amazon Echo', 'Smart Home', 99.99),
    (107, 'Google Nest Hub', 'Smart Home', 129.99),
    (108, 'Canon EOS M50', 'Cameras', 649.99),
    (109, 'Bose SoundLink', 'Audio', 179.99),
    (110, 'Apple Watch', 'Wearables', 399.99),
    (111, 'Fitbit Charge 5', 'Wearables', 149.99),
    (112, 'Nintendo Switch', 'Gaming', 299.99),
    (113, 'MacBook Air', 'Computers', 999.99),
    (114, 'Lenovo ThinkPad', 'Computers', 849.99),
    (115, 'GoPro Hero10', 'Cameras', 499.99);

