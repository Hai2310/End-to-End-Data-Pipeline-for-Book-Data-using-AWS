import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import Window
import time 
import traceback

class DataClean :
    def work_clean(works) :
        print("\n" + "="*60 )
        print("====== Starting Clean works ======")
        print("="*60 )
        # Chọn các cột cần để phân tích
        works = works.select("key", "title", "first_publish_year", "edition_count", "subject", "authors" )
        print("====== Missing Value in works ======")
        works.select([count(when (col(c).isNull() , c)).alias(c) for c in works.columns ]).show()
        # Làm sạch cột key và first_pubnlish_year 
        work_key = works.withColumn("key" , regexp_extract(col("key") , r"[A-Z]+\d+[A-Z]", 0)) \
                        .withColumnRenamed("key", "work_id") \
                        .withColumn("first_publish_year" , col("first_publish_year").cast("int")) # chuyển đổi kiểu dữ liệu cột first_publish_year
        # Xử lý giá trị missing value bằng cách lấy trung vị
        median = work_key.approxQuantile("first_publish_year", [0.5], 0.01)[0]
        work_key = work_key.fillna({"first_publish_year" : int(median)})
        # Explode cột subject 
        work_explode_sub = work_key.withColumn("subject" , regexp_replace("subject" , r"[\]\[]", "")) \
                                .withColumn("subject", explode(split("subject" , ",")))
        # Làm sạch cột subject
        work_explode_sub = work_explode_sub.withColumn("subject", regexp_replace("subject", r'form:|genre:|"', ""))
        # Làm sạch bảng work_explode_sub
        work_explode_sub = work_explode_sub.filter(col("subject").isNotNull() & ~col("subject").rlike("\\\\|[0-9]"))
        # Xóa bỏ khoảng trắng, lọc các dữ liệu sạch
        work_explode_sub = work_explode_sub.orderBy("subject") \
                .withColumn("subject", trim(col("subject"))) \
                .filter(~col("subject").rlike("&|.com$|@|^:|^\\.") & trim(col("subject") != "") ) \
                .withColumn("subject", regexp_replace("subject" ,"'|\\*|-" , "")) 
        # Làm sạch bảng work_explode_sub
        work_explode_sub = work_explode_sub.filter(col("subject").isNotNull() & ~col("subject").rlike("\\\\|[0-9]"))
        # Xóa bỏ khoảng trắng, lọc các dữ liệu sạch
        work_explode_sub = work_explode_sub.orderBy("subject") \
                .withColumn("subject", trim(col("subject"))) \
                .filter(~col("subject").rlike("&|.com$|@|^:|^\\.") & trim(col("subject") != "") ) \
                .withColumn("subject", regexp_replace("subject" ,"'|\\*|-" , "")) 
        work_explode_sub.show(5)
        print("====== Clean works sucessfully ======")
        return work_explode_sub

    def edition_clean(editions) :
        print("\n" + "="*60 )
        print("====== Starting Clean editions ======")
        print("="*60 )
        editions = editions.select("key", "works", "title", "publish_date", "publishers", "languages", "number_of_pages", "isbn_13")
        # Tạo work schema
        work_schema = ArrayType(
            StructType([
                StructField("key", StringType(), True)
            ])
        )
        # Làm sạch cột works trích xuất ra work_id
        editions_clean_work = editions.withColumn("works", from_json("works", work_schema)) \
                                    .withColumn("works", regexp_extract(col("works")[0]["key"], r"[A-Z]+\d+[A-Z]", 0))
        # Chuyển đổi kiểu dữ liệu publish_date và làm sạch cột key (cột edition_id)
        editions_clean = editions_clean_work.withColumn("publish_date", col("publish_date").cast("string")) \
                        .withColumn("key", regexp_extract("key", r"[A-Z]+\d+[A-Z]", 0))
        # Làm sạch cột publish_date
        publish_date_clean = editions_clean.withColumn("publish_date", trim(regexp_extract("publish_date", r"(\d{4})", 1))) \
                                            .withColumn("publish_date", when(col("publish_date") == "", None)
                                                                        .otherwise(col("publish_date").cast("int"))) 
        # Lấy trung vị cột publish_date
        publish_date_median = publish_date_clean.approxQuantile("publish_date", [0.5], 0.01)[0]

        # Lấy trung vị cột number_of_pages
        number_of_pages_median = publish_date_clean.approxQuantile("number_of_pages", [0.5], 0.01)[0]

        # Làm sạch giá trị null các cột
        editions_clean = publish_date_clean.fillna({"publish_date" : publish_date_median, "number_of_pages" : number_of_pages_median,
                                                    "publishers" : "unknown", "languages" : "unknown", "title" : "unknown"})
        editions_clean.select([count(when (col(c).isNull() , c)).alias(c) for c in editions.columns ]).show()
        # Làm sạch cột title
        editions_clean = editions_clean.filter(col("title").rlike("[A-Za-z]")) \
                                        .withColumnRenamed("key", "edition_id") \
                                        .withColumnRenamed("works", "work_id")
        print("Edition count : " , editions_clean.count())
        editions_clean.show(5)
        print("====== Clean editions sucessfully ======")
        return editions_clean

    def author_clean(authors) :
        print("\n" + "="*60 )
        print("====== Starting Clean authors ======")
        print("="*60 )
        # Làm sạch cột author_id
        authors_key = authors.withColumn("key", trim(regexp_replace("key", r"/authors/", ""))) \
                            .withColumnRenamed("key", "author_id")
        # Làm sạch cột name
        authors_name = authors_key.filter((col("name").isNotNull()) & (~col("name").rlike("n/a")))
        # Làm sạch cột birth_date 
        author_clean = authors_name.withColumn("birth_date", trim(regexp_extract("birth_date", r"\d{4}", 0)).cast("int")) \
                                    .withColumn("birth_date", when(col("birth_date").isNull() ,(rand() * 200 + 1800).cast("int")) \
                                                                    .otherwise(col("birth_date")))
        print("====== Clean authors sucessfully ======")
        author_clean.show(5)
        return author_clean