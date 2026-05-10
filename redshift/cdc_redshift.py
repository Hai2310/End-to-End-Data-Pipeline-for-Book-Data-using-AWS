import redshift_connector
class RedshiftCDC :
    def __init__(self):
        self.conn = redshift_connector.connect(
            host = "default-workgroup.222991798269.ap-southeast-1.redshift-serverless.amazonaws.com",
            database = "dev",
            user = "admin",
            password = "MinhHai231005",
            port = 5439
        )

        self.cursor = self.conn.cursor()
        self.iam_role = 'arn:aws:iam::222991798269:role/redshift-s3-role'
        self.s3_path = 's3://mhai-bk/data_warehouse'
    
    # Generic copy + merge data from s3
    def merge_table(self, 
                    table_name,
                    key_column,
                    columns) :
        staging_table = f"{table_name}_staging"
        print("\n" + "="*60)
        print(f"====== START CDC FOR {table_name} ======")
        print("="*60)
        try :
            # Xóa data đang có trong bảng tạm
            print(f"====== TRUNCATE TABLE {table_name} ======")
            truncate_query = f"""TRUNCATE TABLE staging.{staging_table};"""
            self.cursor.execute(truncate_query)

            print(f"====== TRUNCATED {staging_table} ======")

            # Load data từ S3 đến Redshift
            print(f"====== START COPY TABLE {table_name} FROM S3 ======")
            copy_s3_redshift_query = f"""
                                COPY staging.{staging_table}
                                FROM '{self.s3_path}/{table_name}/'
                                IAM_ROLE '{self.iam_role}'
                                FORMAT AS PARQUET ;
                                """
            self.cursor.execute(copy_s3_redshift_query)
            print(f"====== COPIED DATA INTO {staging_table}  ======")

            # CDC MERGE
            print(f"====== START MERGE INTO TABLE {table_name} FROM {staging_table} ======")

            # Cập nhật các cột ở bảng chính khi có data thay đổi ở bảng staging
            update_query = ",\n".join(f"{col} = source.{col}" for col in columns if col != f"{key_column}")

            # Thêm data từ bảng tạm nếu bảng chính chưa có
            insert_query_tgr = ",\n".join(columns) 
            insert_query_src = ",\n".join(f"source.{col}" for col in columns)
            if table_name not in ["work_author", "work_subject"] : 
                merge_query = f"""
                MERGE INTO public.{table_name}
                USING (SELECT  * 
                    FROM (
                            SELECT *,
                                    ROW_NUMBER() OVER(PARTITION BY {key_column} ORDER BY {key_column}) AS RN
                            FROM staging.{staging_table}) t
                    WHERE RN = 1
                    ) source
                ON public.{table_name}.{key_column} = source.{key_column}

                WHEN MATCHED THEN
                UPDATE SET
                    {update_query}

                WHEN NOT MATCHED THEN
                INSERT (
                    {insert_query_tgr}
                )
                VALUES (
                    {insert_query_src}
                );
                """
                self.cursor.execute(merge_query)
            else :
                truncate = f"""TRUNCATE TABLE public.{table_name};"""
                self.cursor.execute(truncate)
                
                insert_query = f"""
                INSERT INTO public.{table_name}
                SELECT DISTINCT 
                        {insert_query_tgr}
                FROM staging.{staging_table} ;
                """
                self.cursor.execute(insert_query)

            self.conn.commit()
            print(f"====== CDC COMLETED FOR {table_name} ======")

        except Exception as e :
            self.conn.rollback()
            print(f"Error in {table_name} : ", e)
        
    def run_etl(self) :

        # DIM_WORK
        self.merge_table(table_name = "dim_work",
                         key_column ="work_id",
                         columns = [
                            "work_id",
                            "title",
                            "first_publish_year",
                            "edition_count"
                            ])
        
        # DIM EDITION
        self.merge_table(table_name = "dim_edition",
                         key_column ="edition_id",
                         columns = [
                            "edition_id",
                            "title",
                            "publish_date",
                            "publisher",
                            "language"
                            ])
        
        # DIM AUTHOR 
        self.merge_table(table_name = "dim_author",
                         key_column ="author_id",
                         columns = [
                            "author_id",
                            "name",
                            "birth_date"
                            ])
        
        # DIM TIME
        self.merge_table(table_name = "dim_time",
                         key_column ="time_id",
                         columns = [
                            "time_id",
                            "publish_year",
                            "decade",
                            "century"
                            ])

        # DIM SUBJECT 
        self.merge_table(table_name = "dim_subject",
                         key_column ="subject_id",
                         columns = [
                            "subject",
                            "subject_id"
                            ])
        
        # WORK SUBJECT
        self.merge_table(table_name = "work_subject",
                         key_column ="subject_id",
                         columns = [
                            "work_id",
                            "subject_id"
                            ])
        
        # WORK AUTHOR 
        self.merge_table(table_name = "work_author",
                         key_column ="author_id",
                         columns = [
                            "work_id",
                            "author_id"
                            ])
        
        # FACT BOOK
        self.merge_table(table_name = "fact_book",
                         key_column ="edition_id",
                         columns = [
                            "edition_id",
                            "work_id",
                            "time_id",
                            "number_of_pages"
                            ])

    def close(self) :    
        self.cursor.close()
        self.conn.close()
        print("====== Redshift connection closed")

        
def main() :
    print("\n" + "="*60)
    print(f"====== START CDC ======")
    print("="*60)

    cdc = RedshiftCDC()
    cdc.run_etl()

    print("\n" + "="*60)
    print(f"====== ALL CDC COMPLETED ======")
    print("="*60)

    cdc.close()

main()





