import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import Window
import time 
import traceback
from configuration import SparkConfig
from pathlib import Path

class DataLoader :
    def __init__(self) :
        self.spark = SparkConfig.create_sparksession() 
    def read_works() :    
        s3 = boto3.client("s3")
        response = s3.list_objects_v2(Bucket = "mhai-bk" , Prefix = "raw_data/works/")
        files = [f"s3://mhai-bk/{item['Key']}" for item in response['Contents']]
        list_file = []
        list_col = []
        for file in files :
            works = self.spark.read.format("parquet").load(file) 
            
            works = works.withColumn("cover_id" , col("cover_id").cast("long"))
            for c in works.columns :
                if c not in list_col :
                    list_col.append(c)
            for c in list_col :
                if c not in works.columns :
                    works = works.withColumn(c , lit(None).cast("string"))
            list_file.append(works)
        works = reduce(lambda a,b : a.union(b) , list_file)
        works.coalesce(1).write.mode("overwrite").parquet("s3://mhai-bk/data/works/")
        return works

    def read_editions() :    
        s3 = boto3.client("s3")
        response = s3.list_objects_v2(Bucket = "mhai-bk" , Prefix = "raw_data/editions/")
        files = [f"s3://mhai-bk/{item['Key']}" for item in response['Contents']]
        list_file = []
        list_col = []
        for file in files :
            editions = self.spark.read.format("parquet").load(file) 
            if "number_of_pages" in editions.columns :
                editions = editions.withColumn("number_of_pages" , col("number_of_pages").cast("long"))
            
            for c in editions.columns :
                if c not in list_col :
                    list_col.append(c)
            for c in list_col :
                if c not in editions.columns :
                    editions = editions.withColumn(c , lit(None).cast("string"))
            list_file.append(editions)
        editions = reduce(lambda a,b : a.union(b) , list_file)
        editions.coalesce(1).write.mode("overwrite").parquet("s3://mhai-bk/data/editions/")
        return editions 
    
    def read_authors() :        
        s3 = boto3.client("s3")
        response = s3.list_objects_v2(Bucket = "mhai-bk" , Prefix = "raw_data/authors/")
        files = [f"s3://mhai-bk/{item['Key']}" for item in response['Contents']]
        list_file = []
        list_col = []
        for file in files :
            authors = self.spark.read.format("parquet").load(file) 
            if "number_of_pages" in authors.columns :
                authors = authors.withColumn("number_of_pages" , col("number_of_pages").cast("long"))
            
            list_file.append(authors)
        authors = reduce(lambda a,b : a.unionByName(b , allowMissingColumns = True) , list_file)
        authors.coalesce(1).write.mode("overwrite").parquet("s3://mhai-bk/data/authors/")
        return authors
    

