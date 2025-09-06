DROP DATABASE superstore;
CREATE DATABASE superstore;
USE superstore;


CREATE TABLE dim_Customer (
    customer_id INT PRIMARY KEY,
    customer_name VARCHAR(60),
    market_value VARCHAR(30),
    segment_type VARCHAR(45)
);
ALTER TABLE dim_Customer 
ADD COLUMN version INT DEFAULT 1,
ADD COLUMN date_from DATE,
ADD COLUMN date_to DATE;

CREATE TABLE dim_Product (
    product_id INT PRIMARY KEY,
    product_name VARCHAR(255),
    category_name VARCHAR(45),
    sub_category_name VARCHAR(45)
);

CREATE TABLE dim_Location (
    location_id INT PRIMARY KEY,
    region_name VARCHAR(45),
    city VARCHAR(60),
    country VARCHAR(60),
    state VARCHAR(60)
);

CREATE TABLE dim_Date (
    date_id INT PRIMARY KEY AUTO_INCREMENT,
    full_date DATE,
    day INT,
    month INT,
    quarter INT,
    year INT
);

CREATE TABLE Fact_Orders (
    order_id INT PRIMARY KEY,
    customer_id INT,
    product_id INT,
    location_id INT,
    order_date_id INT,
    ship_date_id INT,
    sales FLOAT,
    profit FLOAT,
    discount FLOAT,
    quantity INT,
    shipping_cost FLOAT,
    FOREIGN KEY (customer_id) REFERENCES dim_Customer(customer_id),
    FOREIGN KEY (product_id) REFERENCES dim_Product(product_id),
    FOREIGN KEY (location_id) REFERENCES dim_Location(location_id),
    FOREIGN KEY (order_date_id) REFERENCES dim_Date(date_id),
    FOREIGN KEY (ship_date_id) REFERENCES dim_Date(date_id)
);

 SELECT * FROM superstore.Fact_Orders;
 SELECT * FROM customer