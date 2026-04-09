DROP DATABASE superstore;
CREATE DATABASE superstore;
USE superstore;

SELECT c.customer_name, m.market_value, SUM(o.sales) as total_sales
FROM orders o
JOIN customer c ON o.customer_id = c.id
JOIN market m ON c.market_id = m.id
GROUP BY c.customer_name, m.market_value
ORDER BY total_sales DESC
LIMIT 10;

DROP TABLE IF EXISTS fact_orders;
DROP TABLE IF EXISTS dim_Product;
DROP TABLE IF EXISTS dim_Shipping;
DROP TABLE IF EXISTS dim_Location;
DROP TABLE IF EXISTS dim_Customer;
DROP TABLE IF EXISTS dim_Time;


CREATE TABLE dim_Customer (
    customer_id BIGINT NOT NULL PRIMARY KEY,
    version INT,
    date_from DATETIME,
    date_to DATETIME,
    customer_id_1 VARCHAR(100),
    customer_name VARCHAR(255),
    market_value VARCHAR(100),
    segment_type VARCHAR(100)
);

CREATE TABLE dim_Location (
    location_tk INT AUTO_INCREMENT PRIMARY KEY,
    version INT,
    date_from DATE,
    date_to DATE,
    city VARCHAR(255),
    country VARCHAR(255),
    state VARCHAR(255),
    region_name VARCHAR(255)
);

CREATE TABLE dim_Product (
    product_tk INT AUTO_INCREMENT PRIMARY KEY,
    version INT,
    date_from DATETIME,
    date_to DATETIME,
    product_id VARCHAR(100), 
    product_name VARCHAR(255),
    category VARCHAR(255),
    sub_category VARCHAR(255)
);

CREATE TABLE dim_Shipping (
    shipping_tk INT AUTO_INCREMENT PRIMARY KEY,
    ship_mode VARCHAR(255),
    shipping_cost DECIMAL(10,2),
    shipping_priority VARCHAR(50),
    version INT,
    date_from DATETIME,
    date_to DATETIME
);


CREATE TABLE dim_Time (
	date_key INT PRIMARY KEY,
    full_date DATE,
    day_of_week INT,
    day_name VARCHAR(50),
    month_name  VARCHAR(50),
    month_number INT,
    year INT,
    quarter INT
);

CREATE TABLE fact_orders (
    row_id INT AUTO_INCREMENT PRIMARY KEY,
    order_id VARCHAR(25),
    customer_key BIGINT,
    product_key INT,
    shipping_key INT,
    location_key INT,
    sales DECIMAL(15,2),
    quantity INT,
    profit DECIMAL(15,2),
    order_date DATETIME,
    CONSTRAINT fk_customer FOREIGN KEY (customer_key) REFERENCES dim_Customer(customer_id),
    CONSTRAINT fk_location FOREIGN KEY (location_key) REFERENCES dim_Location(location_tk),
    CONSTRAINT fk_shipping FOREIGN KEY (shipping_key) REFERENCES dim_Shipping(shipping_tk),
    CONSTRAINT fk_product FOREIGN KEY (product_key) REFERENCES dim_Product(product_tk),
    CONSTRAINT fk_time FOREIGN KEY (time_key) REFERENCES dim_Time(date_key)
);

/* TEST */
SELECT * FROM fact_orders LIMIT 10;
SELECT * FROM dim_location LIMIT 10;

SELECT shipping_tk, ship_mode, shipping_cost, shipping_priority 
	FROM dim_shipping 
	ORDER BY shipping_tk ASC;
    
SELECT f.order_id, f.sales, s.ship_mode, s.shipping_cost, s.shipping_priority
	FROM fact_orders f
	JOIN dim_shipping s ON f.shipping_key = s.shipping_tk
	LIMIT 10;

SELECT p.product_name, c.customer_name, f.sales, f.profit 
	FROM fact_orders f
	JOIN dim_Product p ON f.product_key = p.product_tk
	JOIN dim_Customer c ON f.customer_key = c.customer_id 
	WHERE f.sales > 1000;

SELECT * FROM dim_time 
	ORDER BY full_date ASC 
	LIMIT 10;

SELECT  l.region_name, SUM(f.sales) AS ukupna_prodaja, COUNT(f.location_key) AS broj_narudzbi
	FROM superstore.fact_orders f
	JOIN superstore.dim_location l ON f.location_key = l.location_tk
	GROUP BY l.region_name
	ORDER BY ukupna_prodaja DESC;
    
SELECT f.order_id, f.sales, l.city, l.region_name, l.version
	FROM fact_orders f
	JOIN dim_location l ON f.location_key = l.location_tk
	LIMIT 10;
    

    
    












