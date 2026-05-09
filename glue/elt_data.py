import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import Window
import time 
import traceback

class EtlPipeline :
    def dim_work(work_clean):
        print("\n" + "="*60 )
        print("====== Create dimension work  ======")
        print("="*60 )
        # Tạo ra bảng dim_work 
        dim_work = work_clean.select("work_id", 
                                    "title", 
                                    "first_publish_year", 
                                    "edition_count").dropDuplicates()
        return dim_work
    def dim_subject(work_clean) :
        print("\n" + "="*60 )
        print("====== Create dimension subject  ======")
        print("="*60 )
        # Tạo bảng dim_subject
        dim_subject = work_clean.select("subject").dropDuplicates()
        dim_subject = dim_subject.distinct()
        # Gen Id cho bảng dim_subject sử dụng window function
        dim_subject = dim_subject.withColumn("subject_id", row_number().over(Window.orderBy("subject")))
        return dim_subject 

    def work_subject(work_clean, dim_subject) :
        print("\n" + "="*60 )
        print("====== Create bridghe work subject table  ======")
        print("="*60 )
        # Tạo bảng bridge work_subject
        work_subject = work_clean.join(dim_subject , on = "subject", how = "inner") \
                        .select("work_id", "subject_id") \
                        .orderBy("subject")
        return work_subject
    def work_author(work_clean):
        print("\n" + "="*60 )
        print("====== Create bridghe work author table  ======")
        print("="*60 )
        # Tạo author schema
        author_schema = ArrayType(
            StructType([
                StructField("key", StringType(), True),
                StructField("name", StringType(), True)
            ])
        )
        # Trích ra cột id và name của author
        work_author = work_clean.withColumn("authors" , from_json(col("authors"), author_schema)) \
                        .withColumn("authors", explode(col("authors"))) \
                        .select("work_id","authors.key")
        # Làm sạch cột author_id
        work_author = work_author.withColumn("key", regexp_extract(col("key"), r"[A-Z]+\d+[A-Z]", 0))
        work_author = work_author.select(col("work_id"), col("key").alias("author_id"))
        return work_author
    ## Edition
    def dim_edition(edition_clean) :
        print("\n" + "="*60 )
        print("====== Create dimension edition ======")
        print("="*60 )
        # Tạo bảng dim_editions
        dim_edition = edition_clean.select("edition_id", "title", "publish_date", "publishers", "languages").dropDuplicates() 
        # Explode cột publishers
        dim_edition_clean = dim_edition.withColumn("publishers", from_json("publishers", ArrayType(StringType()))) \
                                        .withColumn("publishers", explode("publishers"))
        # Xóa các khoảng trắng, lọc các dữ liệu bẩn cột publishers
        dim_edition_clean = dim_edition_clean.withColumn("publishers", trim(regexp_replace("publishers", r".$|-$|\?|\]|\[|s\.n\.?|etc\.?|et al\.?|publisher not identified", ""))) \
                                            .filter( (col("publishers").rlike("[a-z]{3,}"))
                                                    & (col("publishers") != "")
                                                    & (length("publishers") < 86)
                                                    & (~col("publishers").rlike(r"printed by|printed for|distributed by|distributor|sold by")))
        # Tạo schema cho cột languages
        language_schema = ArrayType(
            StructType([
                StructField("key", StringType(), True)
            ])
        )
        # Trích xuất ra language từ cột languages
        dim_edition_clean = dim_edition_clean.withColumn("languages", from_json("languages", language_schema)) \
                                            .withColumn("languages" , regexp_extract(col("languages")[0]["key"], "[A-Z|a-z]+$", 0))
        # Đổi các giá trị null sang unknown
        dim_edition_clean = dim_edition_clean.fillna({"languages" : "unknown"})
        # Đổi tên cột publishers, languages
        dim_edition_clean = dim_edition_clean.withColumnRenamed("publishers" , "publisher") \
                                            .withColumnRenamed("languages" , "language")
        language_map = {
            "eng": "English",
            "vie": "Vietnamese",
            "fre": "French",
            "ger": "German",
            "spa": "Spanish",
            "ita": "Italian",
            "jpn": "Japanese",
            "kor": "Korean",
            "chi": "Chinese",
            "rus": "Russian",
            "por": "Portuguese",
            "ara": "Arabic",
            "hin": "Hindi",
            "ben": "Bengali",
            "tur": "Turkish",
            "pol": "Polish",
            "ukr": "Ukrainian",
            "unknown" : "Unknown"
        }
        from itertools import chain
        # Tạo mapping để map cột language sang ngôn ngữ chi tiết
        mapping = create_map(
            [lit(x) for x in chain(*language_map.items())]
        )
        # Mapping cột language
        dim_edition = dim_edition_clean.withColumn("language", mapping[col("language")])
        return dim_edition
    def dim_time(edition_clean) :
        print("\n" + "="*60 )
        print("====== Create dimension time ======")
        print("="*60 )
        dim_time = edition_clean.select(col("publish_date").alias("publish_year")).distinct()
        # Tạo ra 2 cột decade và century
        dim_time = dim_time.withColumn("decade" , (col("publish_year")/10).cast("int") * 10) \
                .withColumn("century", ((col("publish_year") - 1)/100).cast("int") + 1)
        # Gen ra time_id cho dim_time
        dim_time = dim_time.filter(col("publish_year") >= 1000) \
                            .withColumn("time_id" , row_number().over(Window.orderBy("publish_year"))) \
                            .select("time_id", "publish_year", "decade", "century")
        return dim_time
    def fact_book(edition_clean, dim_time):
        print("\n" + "="*60 )
        print("====== Create fact book ======")
        print("="*60 )
        # Tạo bảng fact_book
        fact_book = edition_clean.select("edition_id", "work_id", col("publish_date").alias("publish_year"), "number_of_pages")
        # Join với bảng dim_time lấy time_id
        fact_book = fact_book.join(broadcast(dim_time.select("time_id", "publish_year")), on = "publish_year", how = "left") \
                            .drop("publish_year") \
                            .select("edition_id", "work_id", "time_id", "number_of_pages")
        return fact_book
    # Author
    def dim_author(author_clean) :
        print("====== Create author dimension ======")
        # Tạo bảng dim_author
        dim_author = author_clean.select("author_id", 
                                "name", 
                                "birth_date").dropDuplicates()
        return dim_author