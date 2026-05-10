-- DROP ALL TABLES
DROP TABLE IF EXISTS public.fact_book;
DROP TABLE IF EXISTS public.dim_work;
DROP TABLE IF EXISTS public.dim_subject;
DROP TABLE IF EXISTS public.work_subject;
DROP TABLE IF EXISTS public.work_author;
DROP TABLE IF EXISTS public.dim_edition;
DROP TABLE IF EXISTS public.dim_time;
DROP TABLE IF EXISTS public.dim_author;

DROP TABLE IF EXISTS staging.fact_book_staging;
DROP TABLE IF EXISTS staging.dim_work_staging;
DROP TABLE IF EXISTS staging.dim_subject_staging;
DROP TABLE IF EXISTS staging.work_subject_staging;
DROP TABLE IF EXISTS staging.work_author_staging;
DROP TABLE IF EXISTS staging.dim_edition_staging;
DROP TABLE IF EXISTS staging.dim_time_staging;
DROP TABLE IF EXISTS staging.dim_author_staging;

-- CREATE STAGING SCHEMA
CREATE SCHEMA IF NOT EXISTS staging;

-- REAL TABLES-- DIM WORK
CREATE TABLE public.dim_work (
    work_id VARCHAR(50) PRIMARY KEY,
    title VARCHAR(10000), 
   first_publish_year INTEGER,
    edition_count BIGINT
);

-- DIM SUBJECT
CREATE TABLE public.dim_subject (
    subject_id INTEGER PRIMARY KEY,
    subject VARCHAR(500)
);

-- WORK SUBJECT
CREATE TABLE public.work_subject (
    work_id VARCHAR(50),
    subject_id INTEGER
);

-- WORK AUTHOR
CREATE TABLE public.work_author (
    work_id VARCHAR(50),
    author_id VARCHAR(50)
);

-- DIM EDITION
CREATE TABLE public.dim_edition (
    edition_id VARCHAR(50) PRIMARY KEY,
    title VARCHAR(1000),
    publish_date INTEGER,
    publisher VARCHAR(500),
    language VARCHAR(100)
);

-- DIM TIME
CREATE TABLE public.dim_time (
    time_id INTEGER PRIMARY KEY,
    publish_year INTEGER,
    decade INTEGER,
    century INTEGER
);

-- DIM AUTHOR
CREATE TABLE public.dim_author (
    author_id VARCHAR(50) PRIMARY KEY,
    name VARCHAR(500),
    birth_date VARCHAR(100)
);

-- FACT BOOK
CREATE TABLE public.fact_book (
    edition_id VARCHAR(50),
    work_id VARCHAR(50),
    time_id INTEGER,
    number_of_pages BIGINT
)
DISTKEY(work_id)
SORTKEY(time_id);

-- STAGING TABLES-- HAVE year month day
-- DIM WORK STAGING
CREATE TABLE staging.dim_work_staging (
    work_id VARCHAR(50),
    title VARCHAR(10000),
    first_publish_year INTEGER,
    edition_count BIGINT
);

-- DIM SUBJECT STAGING
CREATE TABLE staging.dim_subject_staging (
    subject_id INTEGER,
    subject VARCHAR(500)
);

-- WORK SUBJECT STAGING
CREATE TABLE staging.work_subject_staging (
    work_id VARCHAR(50),
    subject_id INTEGER
);

-- WORK AUTHOR STAGING
CREATE TABLE staging.work_author_staging (
    work_id VARCHAR(50),
    author_id VARCHAR(50)
);

-- DIM EDITION STAGING
CREATE TABLE staging.dim_edition_staging (
    edition_id VARCHAR(50),
    title VARCHAR(1000),
    publish_date INTEGER,
    publisher VARCHAR(500),
    language VARCHAR(100)
);

-- DIM TIME STAGING
CREATE TABLE staging.dim_time_staging (
    time_id INTEGER,
    publish_year INTEGER,
    decade INTEGER,
    century INTEGER
);

-- DIM AUTHOR STAGING
CREATE TABLE staging.dim_author_staging (
    author_id VARCHAR(50),
    name VARCHAR(500),
    birth_date INTEGER
);

-- FACT BOOK STAGING
CREATE TABLE staging.fact_book_staging (
    edition_id VARCHAR(50),
    work_id VARCHAR(50),
    time_id INTEGER,
    number_of_pages BIGINT
);