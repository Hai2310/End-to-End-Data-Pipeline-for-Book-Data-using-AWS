import redshift_connector

conn = redshift_connector.connect(
    host='your-redshift-cluster.xxxxxx.ap-southeast-1.redshift.amazonaws.com',
    database='dev',
    user='admin',
    password='your_password',
    port=5439
)

cursor = conn.cursor()

# Xóa staging cũ
cursor.execute("""
TRUNCATE TABLE public.fact_book_staging;
""")

# COPY parquet mới từ S3
copy_query = """
COPY public.fact_book_staging
FROM 's3://mhai-bk/warehouse/fact_book/'
IAM_ROLE 'arn:aws:iam::123456789012:role/redshift-s3-role'
FORMAT AS PARQUET;
"""

cursor.execute(copy_query)

# CDC MERGE
merge_query = """
MERGE INTO public.fact_book AS target
USING public.fact_book_staging AS source
ON target.edition_id = source.edition_id

WHEN MATCHED THEN
UPDATE SET
    work_id = source.work_id,
    time_id = source.time_id,
    number_of_pages = source.number_of_pages

WHEN NOT MATCHED THEN
INSERT (
    edition_id,
    work_id,
    time_id,
    number_of_pages
)
VALUES (
    source.edition_id,
    source.work_id,
    source.time_id,
    source.number_of_pages
);
"""

cursor.execute(merge_query)

conn.commit()

print("CDC completed successfully")

cursor.close()
conn.close()