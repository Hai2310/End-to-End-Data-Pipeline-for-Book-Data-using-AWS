import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import Window
import time 
import traceback
from pathlib import Path
import boto3
import functools
from configuration import SparkConfig

class DataLoader :
    def __init__(self) :
        self.spark = SparkConfig.create_sparksession() 
    def read_works(self) :
        print("\n" + "="*60 )
        print("====== Start Loading works ======")
        print("="*60 )
        s3 = boto3.client("s3")
        response = s3.list_objects_v2(Bucket = "mhai-bk" , Prefix = "raw_data/works/")
        files = [f"s3://mhai-bk/{item['Key']}" for item in response['Contents']]
        list_file = []
        list_col = []
        for file in files :
            works = self.spark.read.format("parquet").load(file) 
            
            works = works.withColumn("cover_id" , col("cover_id").cast("long"))
            list_file.append(works)
        works = functools.reduce(lambda a,b : a.unionByName(b , allowMissingColumns = True) , list_file)
        print("====== Loaded works sucessfully ======")
        return works

    def read_editions(self) :
        print("\n" + "="*60 )
        print("====== Start Loading editions ======")
        print("="*60 )
        s3 = boto3.client("s3")
        response = s3.list_objects_v2(Bucket = "mhai-bk" , Prefix = "raw_data/editions/")
        files = [f"s3://mhai-bk/{item['Key']}" for item in response['Contents']]
        list_file = []
        list_col = []
        for file in files :
            editions = self.spark.read.format("parquet").load(file) 
            if "number_of_pages" in editions.columns :
                editions = editions.withColumn("number_of_pages" , col("number_of_pages").cast("long"))
            
            list_file.append(editions)
        editions = functools.reduce(lambda a,b : a.unionByName(b , allowMissingColumns = True) , list_file)
        print("====== Loaded editions sucessfully ======")
        return editions 
    
    def read_authors(self) : 
        print("\n" + "="*60 )
        print("====== Start Loading authors ======")
        print("="*60 )
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
        authors = functools.reduce(lambda a,b : a.unionByName(b , allowMissingColumns = True) , list_file)
        print("====== Loaded authors sucessfully ======")
        return authors
    