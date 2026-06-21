-- DROP TABLE
DROP TABLE fact_sales;

DROP TABLE dim_customer;
DROP TABLE dim_product;
DROP TABLE dim_seller;
DROP TABLE dim_review;
DROP TABLE dim_payment;
DROP TABLE dim_date;



-- DIMENSION TABLES
CREATE TABLE dim_customer (
    customer_id VARCHAR(50) PRIMARY KEY,
    customer_unique_id VARCHAR(50) NOT NULL,
    customer_city VARCHAR(100) NOT NULL,
    customer_state VARCHAR(5) NOT NULL,
    customer_zip_code_prefix VARCHAR(10) NOT NULL,
    geolocation_lat DOUBLE PRECISION NOT NULL,
    geolocation_lng DOUBLE PRECISION NOT NULL,
    customer_state_name VARCHAR(100) NOT NULL
);


CREATE TABLE dim_date (
    date_id VARCHAR(8) PRIMARY KEY,
    full_date DATE NOT NULL,
    year INTEGER NOT NULL,
    quarter INTEGER NOT NULL,
    month INTEGER NOT NULL,
    month_name VARCHAR(20) NOT NULL,
    week INTEGER NOT NULL,
    day INTEGER NOT NULL,
    weekday VARCHAR(20) NOT NULL,
    is_weekend BOOLEAN NOT NULL
);


CREATE TABLE dim_payment (
    payment_id INTEGER PRIMARY KEY,
    payment_type VARCHAR(30) NOT NULL,
    payment_installments INTEGER NOT NULL,
    payment_category VARCHAR(30) NOT NULL
);


CREATE TABLE dim_product (
    product_id VARCHAR(50) PRIMARY KEY,
    product_category VARCHAR(100) NOT NULL,
    product_name_length INTEGER NOT NULL,
    product_description_length INTEGER NOT NULL,
    product_photos_qty INTEGER NOT NULL,
    product_weight_g INTEGER NOT NULL,
    product_length_cm INTEGER NOT NULL,
    product_height_cm INTEGER NOT NULL,
    product_width_cm INTEGER NOT NULL,
    product_volume_cm3 BIGINT NOT NULL
);


CREATE TABLE dim_review (
    review_id VARCHAR(50) PRIMARY KEY,
    review_score INTEGER NOT NULL,
    review_sentiment VARCHAR(20) NOT NULL,
    review_comment_message TEXT,
    review_creation_date TIMESTAMP NOT NULL
);


CREATE TABLE dim_seller (
    seller_id VARCHAR(50) PRIMARY KEY,
    seller_city VARCHAR(100) NOT NULL,
    seller_state VARCHAR(50) NOT NULL,
    seller_zip_code_prefix VARCHAR(10) NOT NULL
);


-- FACT TABLE

CREATE TABLE fact_sales (
    order_id VARCHAR(50) NOT NULL,

    customer_id VARCHAR(50),
    product_id VARCHAR(50) NOT NULL,
    seller_id VARCHAR(50) NOT NULL,
    review_id VARCHAR(50),
    payment_id INTEGER,
    date_id VARCHAR(8),

    order_status VARCHAR(30),

    price NUMERIC(10,2) NOT NULL,
    freight_value NUMERIC(10,2) NOT NULL,
    total_amount NUMERIC(10,2) NOT NULL,
    payment_value NUMERIC(10,2),

    delivery_days INTEGER,
    approval_days INTEGER,

    is_late_delivery BOOLEAN NOT NULL,


    -- FOREIGN KEYS

    CONSTRAINT fk_customer
        FOREIGN KEY (customer_id)
        REFERENCES dim_customer(customer_id),

    CONSTRAINT fk_product
        FOREIGN KEY (product_id)
        REFERENCES dim_product(product_id),

    CONSTRAINT fk_seller
        FOREIGN KEY (seller_id)
        REFERENCES dim_seller(seller_id),

    CONSTRAINT fk_review
        FOREIGN KEY (review_id)
        REFERENCES dim_review(review_id),

    CONSTRAINT fk_payment
        FOREIGN KEY (payment_id)
        REFERENCES dim_payment(payment_id),

    CONSTRAINT fk_date
        FOREIGN KEY (date_id)
        REFERENCES dim_date(date_id)
);

SELECT * FROM FACT_SALES LIMIT 10 ;

