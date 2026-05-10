-- DIM WORK
CREATE TABLE IF NOT EXISTS dim_work (
    work_id VARCHAR(50) PRIMARY KEY,
    title VARCHAR(1000),
    first_publish_year INTEGER,
    edition_count BIGINT
);

-- DIM SUBJECT
CREATE TABLE IF NOT EXISTS dim_subject (
    subject_id INTEGER PRIMARY KEY,
    subject VARCHAR(500)
);

-- BRIDGE WORK SUBJECT
CREATE TABLE IF NOT EXISTS work_subject (
    work_id VARCHAR(50),
    subject_id INTEGER
);

-- BRIDGE WORK AUTHOR
CREATE TABLE IF NOT EXISTS work_author (
    work_id VARCHAR(50),
    author_id VARCHAR(50)
);

-- DIM EDITION
CREATE TABLE IF NOT EXISTS dim_edition (
    edition_id VARCHAR(50) PRIMARY KEY,
    title VARCHAR(1000),
    publish_date INTEGER,
    publisher VARCHAR(500),
    language VARCHAR(100)
);

-- DIM TIME
CREATE TABLE IF NOT EXISTS dim_time (
    time_id INTEGER PRIMARY KEY,
    publish_year INTEGER,
    decade INTEGER,
    century INTEGER
);

-- DIM AUTHOR
CREATE TABLE IF NOT EXISTS dim_author (
    author_id VARCHAR(50) PRIMARY KEY,
    name VARCHAR(500),
    birth_date VARCHAR(100)
);

-- FACT BOOK
CREATE TABLE IF NOT EXISTS fact_book (
    edition_id VARCHAR(50),
    work_id VARCHAR(50),
    time_id INTEGER,
    number_of_pages BIGINT
);

-- STAGING TABLES FOR CDC


-- STAGING TABLES FOR CDC

CREATE TABLE IF NOT EXISTS stagging.dim_work_staging (
    LIKE public.dim_work
);

CREATE TABLE IF NOT EXISTS stagging.dim_subject_staging (
    LIKE public.dim_subject
);

CREATE TABLE IF NOT EXISTS stagging.work_subject_staging (
    LIKE public.work_subject
);

CREATE TABLE IF NOT EXISTS stagging.work_author_staging (
    LIKE public.work_author
);

CREATE TABLE IF NOT EXISTS stagging.dim_edition_staging (
    LIKE public.dim_edition
);

CREATE TABLE IF NOT EXISTS stagging.dim_time_staging (
    LIKE public.dim_time
);

CREATE TABLE IF NOT EXISTS stagging.dim_author_staging (
    LIKE public.dim_author
);

CREATE TABLE IF NOT EXISTS stagging.fact_book_staging (
    LIKE public.fact_book
);
